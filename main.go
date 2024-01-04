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
	endpoint, accessKey, secretKey string
	insecure, dryRun               bool
	objectsDeleted                 int64
	apiPath                        string
)

func main() {
	flag.StringVar(&endpoint, "endpoint", "", "S3 endpoint URL")
	flag.StringVar(&accessKey, "access-key", "", "S3 Access Key")
	flag.StringVar(&secretKey, "secret-key", "", "S3 Secret Key")
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

	s3Client, adminClient := getS3Clients(endpoint, accessKey, secretKey, insecure)
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
		if !strings.HasSuffix(traceInfo.Trace.Path, "/") {
			continue
		}
		if apiPath != "" && !wildcard.Match(path.Join("/", apiPath), traceInfo.Trace.Path) {
			continue
		}
		split := strings.Split(traceInfo.Trace.Path, "/")
		if len(split) <= 2 {
			continue
		}
		bucket := split[0]
		objectKey := strings.TrimSuffix(strings.Join(split[1:], "/"), "/")
		objectKey = objectKey + "__XLDIR__"
		if dryRun {
			fmt.Println(bucket + "/" + objectKey)
			continue
		}
		if err := s3Client.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{
			ForceDelete:      true,
			GovernanceBypass: true,
		}); err != nil {
			log.Printf("unable to delete the object: %v; %v\n", objectKey, err)
		}
	}
	if !dryRun {
		objectsDeleted++
		fmt.Printf("\nDeleted %v objects...", objectsDeleted)
	}
}

func getS3Clients(endpoint string, accessKey string, secretKey string, insecure bool) (*minio.Client, *madmin.AdminClient) {
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

	madmClnt, err := madmin.New(endpoint, accessKey, secretKey, secure)
	if err != nil {
		log.Fatalln(err)
	}
	madmClnt.SetCustomTransport(transport)
	madmClnt.SetAppInfo("traceanddelete", "v1")

	return s3Client, madmClnt
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