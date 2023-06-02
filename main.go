package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	s3Path = flag.String("s3path", "", "S3 path in the format 's3://bucket-name/folder-name'")
	region = flag.String("region", "us-west-1", "AWS region")

	accessKey   = flag.String("accesskey", "", "AWS access key")
	secretKey   = flag.String("secretkey", "", "AWS secret key")
	localDir    = flag.String("localdir", "", "Local directory to download/upload objects")
	concurrency = flag.Int("concurrency", 50, "Number of S3 clients to download/upload objects concurrently")

	upload = flag.Bool("upload", false, "Upload files to S3")
)

func main() {

	flag.Parse()

	// Validate command-line flags
	if *s3Path == "" || *accessKey == "" || *secretKey == "" || *localDir == "" {
		flag.Usage()
		return
	}

	// Parse S3 path
	bucket, prefix := parseS3Path(*s3Path)
	if bucket == "" || prefix == "" {
		fmt.Println("Invalid S3 path")

		return
	}

	// Create AWS configuration with credentials
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*accessKey, *secretKey, "")),
		config.WithRegion(*region))
	if err != nil {
		fmt.Println("Error loading AWS config:", err)

		return
	}

	startTime := time.Now()

	// Create multiple S3 service clients
	s3Clients := make([]*s3.Client, *concurrency)
	for i := 0; i < *concurrency; i++ {
		s3Clients[i] = s3.NewFromConfig(cfg)
	}

	var totalObjNum uint64
	var totalSize uint64

	// Create a wait group for the workers
	var wg sync.WaitGroup

	if *upload {
		// Create object channel and start worker goroutines for uploading
		type ft struct {
			path string
			size int64
		}

		objectChan := make(chan ft)

		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func(client *s3.Client) {
				defer wg.Done()
				for f := range objectChan {
					err := uploadObject(client, bucket, prefix, *s3Path, f.path, *localDir)
					if err != nil {
						fmt.Printf("Error uploading file %s: %v\n", f.path, err)

						continue
					}

					atomic.AddUint64(&totalObjNum, 1)
					atomic.AddUint64(&totalSize, uint64(f.size))

				}
			}(s3Clients[i])
		}

		// Traverse the local directory and push files to the channel for uploading
		err = filepath.Walk(*localDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				objectChan <- ft{path, info.Size()}
			}

			return nil
		})
		if err != nil {
			fmt.Println("Error traversing local directory:", err)

			return
		}

		close(objectChan)
	} else {
		// Create object channel and start worker goroutines for downloading
		objectChan := make(chan types.Object)

		for i := 0; i < *concurrency; i++ {

			wg.Add(1)
			go func(client *s3.Client) {
				defer wg.Done()
				for obj := range objectChan {
					err := downloadObject(client, bucket, *obj.Key, *localDir)
					if err != nil {
						fmt.Printf("Error downloading object %s: %v\n", *obj.Key, err)

						continue
					}

					atomic.AddUint64(&totalObjNum, 1)
					atomic.AddUint64(&totalSize, uint64(obj.Size))

				}
			}(s3Clients[i])
		}

		// Create S3 service client for listing objects
		s3Client := s3.NewFromConfig(cfg)

		// List objects in the specified S3 bucket and prefix
		paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
			Bucket: &bucket,
			Prefix: &prefix,
		})
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.TODO())
			if err != nil {
				fmt.Println("Error listing objects:", err)

				return
			}

			// Push objects to the channel for processing by workers
			for _, obj := range page.Contents {
				objectChan <- obj
			}
		}

		close(objectChan)
	}

	// Wait for all workers to finish
	wg.Wait()

	fmt.Println("Total Objects:", totalObjNum)
	fmt.Printf("Total Size: %d MB\n", totalSize/1024/1024)
	fmt.Printf("Total Duration: %s\n", time.Since(startTime))
	fmt.Printf("Average Throughput: %.2f MB/s", float64(totalSize/1024/1024)/time.Since(startTime).Seconds())

}

// Parses S3 path into bucket and prefix
func parseS3Path(s3Path string) (bucket, prefix string) {
	if !strings.HasPrefix(s3Path, "s3://") {
		return "", ""
	}

	s3Path = s3Path[5:]
	parts := strings.SplitN(s3Path, "/", 2)
	if len(parts) < 2 {
		return "", ""
	}

	return parts[0], parts[1]
}

// Downloads an object from S3 to a local file preserving the directory structure
func downloadObject(s3Client *s3.Client, bucket, objectKey, localDir string) error {
	// Create the directory structure on the local file system
	localPath := filepath.Join(localDir, objectKey)
	if err := os.MkdirAll(filepath.Dir(localPath), os.ModePerm); err != nil {
		return err
	}

	// Create the file
	file, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Download the object
	getObjectInput := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &objectKey,
	}
	getObjectOutput, err := s3Client.GetObject(context.Background(), getObjectInput)
	if err != nil {
		return err
	}
	defer getObjectOutput.Body.Close()

	// Copy the object contents to the file
	_, err = io.Copy(file, getObjectOutput.Body)
	if err != nil {
		return err
	}

	return nil
}

// Uploads a file to S3 preserving the relative path structure
func uploadObject(s3Client *s3.Client, bucket, prefix, s3Path, localFilePath, localDir string) error {
	// Calculate the relative path of the file
	relativePath, err := filepath.Rel(localDir, localFilePath)
	if err != nil {
		return err
	}

	// Construct the S3 object key using the prefix and relative path
	objectKey := prefix + "/" + relativePath

	// Open the file for reading
	file, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Upload the file to S3
	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &objectKey,
		Body:   file,
	})
	if err != nil {
		return err
	}

	return nil
}
