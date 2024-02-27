```sh
Usage of ./traceanddelete_v3:
  -access-key string
    	S3 Access Key
  -dry-run
    	Enable dry run mode
  -endpoint string
    	S3 endpoint URL
  -insecure
    	Disable TLS verification
  -older-than duration
    	To delete objects older than duration; example: 1h, 1d
  -path string
    	Filter only matching path
  -print-with-prefix string
    	if set, prints the entries with prefix; to be used with dry-run only
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
./traceanddelete_v3 --remote-endpoint http://localhost:9000 --remote-access-key minio --remote-secret-key minio123 --endpoint http://localhost:9002 --access-key minio --secret-key minio123
```
