# s3sync
A benchmark tool to benchmark syncing a dataset between local disk and s3 with concurrent I/O.

## Usage
```
s3sync - help
Usage of s3sync:
  -accesskey string
        AWS access key
  -concurrency int
        Number of S3 clients to download/upload objects concurrently (default 50)
  -localdir string
        Local directory to download/upload objects
  -region string
        AWS region (default "us-west-1")
  -s3path string
        S3 path in the format 's3://bucket-name/folder-name'
  -secretkey string
        AWS secret key
  -upload
        Upload files to S3

```

