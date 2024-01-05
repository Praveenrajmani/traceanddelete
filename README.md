```sh
Usage of ./traceanddelete:
  -access-key string
    	S3 Access Key
  -dry-run
    	Enable dry run mode
  -endpoint string
    	S3 endpoint URL
  -insecure
    	Disable TLS verification
  -path string
    	Filter only matching path
  -remote-access-key string
    	Remote site access Key
  -remote-endpoint string
    	Remote site endpoint URL
  -remote-secret-key string
    	Remote secret Key
  -secret-key string
    	S3 Secret Key
  -workers int
	    Add workers to process the DELETEs (default 5)
```

Example :-

```sh
./traceanddelete --insecure --endpoint https://localhost:9002 --access-key minio --secret-key minio123 --remote-endpoint https://localhost:9000 --remote-access-key minio --remote-secret-key minio123 --dry-run
```
