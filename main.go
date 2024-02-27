package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"

	madmin "github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/pkg/v2/wildcard"
	"github.com/minio/pkg/workers"
)

var (
	endpoint, accessKey, secretKey                   string
	remoteEndpoint, remoteAccessKey, remoteSecretKey string
	insecure, dryRun                                 bool
	objectsDeleted                                   atomic.Int64
	apiPath                                          string
	workerCount                                      int
	olderThan                                        time.Duration
	printWithPrefix                                  string
)

type deleteArgs struct {
	bucket string
	object string
}

func main() {
	flag.StringVar(&endpoint, "endpoint", "", "S3 endpoint URL")
	flag.StringVar(&accessKey, "access-key", "", "S3 Access Key")
	flag.StringVar(&secretKey, "secret-key", "", "S3 Secret Key")
	flag.StringVar(&remoteEndpoint, "remote-endpoint", "", "Remote site endpoint URL")
	flag.StringVar(&remoteAccessKey, "remote-access-key", "", "Remote site access Key")
	flag.StringVar(&remoteSecretKey, "remote-secret-key", "", "Remote secret Key")
	flag.BoolVar(&insecure, "insecure", false, "Disable TLS verification")
	flag.BoolVar(&dryRun, "dry-run", false, "Enable dry run mode")
	flag.StringVar(&apiPath, "path", "", "Filter only matching path")
	flag.IntVar(&workerCount, "workers", 5, "Add workers to process the DELETEs")
	flag.DurationVar(&olderThan, "older-than", 0, "To delete objects older than duration; example: 1h, 1d")
	flag.StringVar(&printWithPrefix, "print-with-prefix", "", "if set, prints the entries with prefix; to be used with dry-run only")
	flag.Parse()

	if endpoint == "" {
		log.Fatalln("endpoint is not provided")
	}
	if accessKey == "" {
		log.Fatalln("access key is not provided")
	}
	if secretKey == "" {
		log.Fatalln("secret key is not provided")
	}
	if remoteEndpoint == "" {
		log.Fatalln("remote endpoint is not provided")
	}
	if remoteAccessKey == "" {
		log.Fatalln("remote access key is not provided")
	}
	if remoteSecretKey == "" {
		log.Fatalln("remote secret key is not provided")
	}

	uniquePathMap := make(map[string]struct{})

	s3Client, remoteS3Client, adminClient := getClients(clientArgs{
		endpoint:        endpoint,
		accessKey:       accessKey,
		secretKey:       secretKey,
		remoteEndpoint:  remoteEndpoint,
		remoteAccessKey: remoteAccessKey,
		remoteSecretKey: remoteSecretKey,
		insecure:        insecure,
	})
	ctx := context.Background()

	if !dryRun {
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					fmt.Printf("\n[PROGRESS] Deleted %v objects...", objectsDeleted.Load())
				}
			}
		}()
	}

	deleteQueueCh := make(chan deleteArgs, workerCount*2)

	wk, err := workers.New(workerCount)
	if err != nil {
		log.Fatalf("unable to create workers; %v", err)
	}

	for i := 0; i < workerCount; i++ {
		wk.Take()
		go func() {
			defer wk.Give()
			for {
				select {
				case args := <-deleteQueueCh:
					fmt.Printf("\nDeleting /%s/%s", args.bucket, args.object)
					if err := s3Client.RemoveObject(ctx, args.bucket, args.object, minio.RemoveObjectOptions{
						ForceDelete:      true,
						GovernanceBypass: true,
					}); err != nil {
						log.Printf("unable to delete the object from source: %v; %v\n", args.object, err)
					}
					if err := remoteS3Client.RemoveObject(ctx, args.bucket, args.object, minio.RemoveObjectOptions{
						ForceDelete:      true,
						GovernanceBypass: true,
					}); err != nil {
						log.Printf("unable to delete the object from remote: %v; %v\n", args.object, err)
					}
					objectsDeleted.Add(1)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	if !dryRun {
		fmt.Println()
		fmt.Printf("Configured Endpoint: '%v'\n", endpoint)
		fmt.Printf("Configured Remote Endpoint: '%v'\n", remoteEndpoint)
		fmt.Printf("Configured Workers: %v\n", workerCount)
		fmt.Println()
		fmt.Println("Listening for HEAD object failures...")
	}

	traceCh := adminClient.ServiceTrace(ctx, madmin.ServiceTraceOpts{S3: true, OnlyErrors: true})
	for traceInfo := range traceCh {
		if traceInfo.Err != nil {
			log.Fatalf("stopped tracing; %v", traceInfo.Err)
		}
		if traceInfo.Trace.FuncName != "s3.HeadObject" {
			continue
		}
		if traceInfo.Trace.HTTP == nil || traceInfo.Trace.HTTP.RespInfo.StatusCode != 405 {
			continue
		}
		if values, ok := traceInfo.Trace.HTTP.RespInfo.Headers["x-amz-delete-marker"]; !ok || len(values) == 0 || values[0] != "true" {
			continue
		}
		isDirMarker := strings.HasSuffix(traceInfo.Trace.Path, "/")

		if apiPath != "" && !wildcard.Match(path.Join("/", apiPath), traceInfo.Trace.Path) {
			continue
		}
		if _, ok := uniquePathMap[traceInfo.Trace.Path]; ok {
			continue
		}
		uniquePathMap[traceInfo.Trace.Path] = struct{}{}
		path, err := url.PathUnescape(traceInfo.Trace.Path)
		if err != nil {
			fmt.Printf("\n[ERROR] unable to escape the path %v; %v", traceInfo.Trace.Path, err)
			continue
		}
		path = strings.TrimPrefix(path, "/")
		split := strings.Split(path, "/")
		if len(split) < 2 {
			continue
		}

		bucket := split[0]

		objectKey := strings.TrimSuffix(strings.Join(split[1:], "/"), "/")
		if isDirMarker {
			objectKey = objectKey + "__XLDIR__"
		}

		if objectKey == "" {
			continue
		}

		if !isDirMarker {
			if !validateDeleteMarker(ctx, s3Client, bucket, objectKey, olderThan) {
				continue
			}
			if !validateDeleteMarker(ctx, remoteS3Client, bucket, objectKey, olderThan) {
				continue
			}
		}

		if dryRun {
			fmt.Println(printWithPrefix + "/" + bucket + "/" + objectKey)
			continue
		}

		select {
		case deleteQueueCh <- deleteArgs{
			bucket: bucket,
			object: objectKey,
		}:
		case <-ctx.Done():
			fmt.Println("context cancelled; %v", ctx.Err())
			break
		}
	}

	// wait for the workers
	wk.Wait()

	if !dryRun {
		fmt.Printf("\n[PROGRESS] Deleted %v objects...", objectsDeleted.Load())
	}
}

type clientArgs struct {
	endpoint, accessKey, secretKey, remoteEndpoint, remoteAccessKey, remoteSecretKey string
	insecure                                                                         bool
}

func getClients(args clientArgs) (*minio.Client, *minio.Client, *madmin.AdminClient) {
	return gets3Client(args.endpoint, args.accessKey, args.secretKey),
		gets3Client(args.remoteEndpoint, args.remoteAccessKey, args.remoteSecretKey),
		gets3AdminClient(args.endpoint, args.accessKey, args.secretKey)
}

func gets3Client(endpoint, accessKey, secretKey string) *minio.Client {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatalln(err)
	}
	secure := strings.EqualFold(u.Scheme, "https")
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		log.Fatalln(err)
	}
	if transport.TLSClientConfig != nil {
		transport.TLSClientConfig.InsecureSkipVerify = insecure
	}
	s3Client, err := minio.New(u.Host, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    secure,
		Transport: transport,
	})
	if err != nil {
		log.Fatalln(err)
	}
	s3Client.SetAppInfo("traceanddelete", "v3.0")
	return s3Client
}

func gets3AdminClient(endpoint, accessKey, secretKey string) *madmin.AdminClient {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatalln(err)
	}
	secure := strings.EqualFold(u.Scheme, "https")
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		log.Fatalln(err)
	}
	if transport.TLSClientConfig != nil {
		transport.TLSClientConfig.InsecureSkipVerify = insecure
	}
	madmClnt, err := madmin.New(u.Host, accessKey, secretKey, secure)
	if err != nil {
		log.Fatalln(err)
	}
	madmClnt.SetCustomTransport(transport)
	madmClnt.SetAppInfo("traceanddelete", "v3.0")
	return madmClnt
}

func validateDeleteMarker(ctx context.Context, client *minio.Client, bucket, object string, longerThan time.Duration) bool {
	soi, err := client.StatObject(ctx, bucket, object, minio.StatObjectOptions{
		Internal: minio.AdvancedGetOptions{ReplicationDeleteMarker: true},
	})
	if err != nil {
		if minio.ToErrorResponse(err).Code != "NoSuchKey" {
			return false
		}
		return true
	}
	// ToDo: This needs to be fixed..
	// The below code is not reachable, the Stat() will always return "NoSuchKey"
	// for objects with latest version as DEL marker..
	// We need to LIST() instead which is costlier...
	if !soi.IsDeleteMarker {
		return false
	}
	if !soi.IsLatest {
		return false
	}
	if longerThan != 0 {
		currentTime := time.Now().UTC()
		if soi.LastModified.After(currentTime.Add(-longerThan)) {
			return false
		}
	}
	return true
}
