package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"path"
	"strings"
	"time"

	madmin "github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/pkg/v2/wildcard"
)

var (
	endpoint, accessKey, secretKey                   string
	remoteEndpoint, remoteAccessKey, remoteSecretKey string
	insecure, dryRun                                 bool
	objectsDeleted                                   int64
	apiPath                                          string
)

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
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					fmt.Printf("\nDeleted %v objects...", objectsDeleted)
				}
			}
		}()
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
		if !strings.HasSuffix(traceInfo.Trace.Path, "/") {
			continue
		}
		if apiPath != "" && !wildcard.Match(path.Join("/", apiPath), traceInfo.Trace.Path) {
			continue
		}
		path := strings.TrimPrefix(traceInfo.Trace.Path, "/")
		split := strings.Split(path, "/")
		if len(split) <= 2 {
			continue
		}
		bucket := split[0]
		objectKey := strings.TrimSuffix(strings.Join(split[1:], "/"), "/")
		objectKey = objectKey + "__XLDIR__"
		if dryRun {
			fmt.Println("/" + bucket + "/" + objectKey)
			continue
		}
		if err := s3Client.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{
			ForceDelete:      true,
			GovernanceBypass: true,
		}); err != nil {
			log.Printf("unable to delete the object from source: %v; %v\n", objectKey, err)
		}
		if err := remoteS3Client.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{
			ForceDelete:      true,
			GovernanceBypass: true,
		}); err != nil {
			log.Printf("unable to delete the object from remote: %v; %v\n", objectKey, err)
		}
		objectsDeleted++
	}
	if !dryRun {
		fmt.Printf("\nDeleted %v objects...", objectsDeleted)
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
	s3Client.SetAppInfo("traceanddelete", "v1")
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
	madmClnt.SetAppInfo("traceanddelete", "v1")
	return madmClnt
}

func logProgress(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("\nDeleted %v objects...", objectsDeleted)
		}
	}
}
