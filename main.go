package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
)

type S3Session struct {
	fileName  []string
	directory string
	session   s3.S3
	s3_sess   session.Session
}

// Contains tells whether a contains x.
func Contains[T comparable](arr []T, x T) bool {
	for _, n := range arr {
		if x == n {
			return true
		}
	}
	return false
}

// Download file to a directory
func (sess *S3Session) DownloadObjectsFromS3() {
	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(&sess.s3_sess)

	resp, err := sess.session.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(os.Getenv("BUCKET_NAME")), Prefix: aws.String(os.Getenv("PREFIX"))})
	if err != nil {
		fmt.Errorf("Unable to list items in bucket %q, %v", os.Getenv("BUCKET_NAME"), err)
	}

	for _, item := range resp.Contents {
		// fmt.Println("Name:         ", *item.Key)
		// fmt.Println("Last modified:", *item.LastModified)
		// fmt.Println("Size:         ", *item.Size)
		// fmt.Println("Storage class:", *item.StorageClass)

		if Contains[string](sess.fileName, *item.Key) {
			filename := *item.Key
			// Create a file to write the S3 Object contents to.
			file, err := os.Create(filename)
			if err != nil {
				fmt.Errorf("failed to create file %q, %v", filename, err)
			}

			numBytes, err := downloader.Download(file,
				&s3.GetObjectInput{
					Bucket: aws.String(os.Getenv("BUCKET_NAME")),
					Key:    aws.String(*item.Key),
				})
			if err != nil {
				fmt.Errorf("Unable to download item %q, %v", item, err)
			}
			fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
		} else {
			fmt.Println("File", *item.Key, "Already present")
		}
	}
}

// Upload file from a directory
func (sess *S3Session) UploadObjectsToS3() {
	// resp, err := sess.session.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(os.Getenv("BUCKET_NAME")), Prefix: aws.String(os.Getenv("PREFIX"))})
	// if err != nil {
	// 	fmt.Errorf("Unable to list items in bucket %q, %v", os.Getenv("BUCKET_NAME"), err)
	// }

	// for _, file := range sess.fileName {
	// 	if Contains[*s3.Object](resp.Contents, file) {

	// 	}
	// }
}

// Delete file from s3
func (F *S3Session) DeleteObjectsFromS3() {

}

// list files from directory
func (f *S3Session) ListAllFilefromDirectory() {
	entries, err := os.ReadDir(f.directory)
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range entries {
		f.fileName = append(f.fileName, e.Name())
	}
}

func (sess *S3Session) S3Session() {
	conf := aws.Config{
		Credentials: credentials.NewStaticCredentials(os.Getenv("ACCESS_KEY_ID"), os.Getenv("SECRET_ACCESS_KEY"), ""),
		// Endpoint:    aws.String(os.Getenv("ENDPOINT")),
		Region: aws.String(os.Getenv("REGION")),
	}

	sessions, err := session.NewSession(&conf)
	if err != nil {
		log.Fatalf("Error connecting to s3: ", err.Error())
	}

	// create s3 session
	log.Println("Connected to S3")
	sess.session = *s3.New(sessions)
	sess.s3_sess = *sessions
}

func main() {
	DIRECTORY := "./"
	s3 := S3Session{}             // instantiate Files struct
	s3.directory = DIRECTORY      // instansiate directory name
	s3.ListAllFilefromDirectory() // call to list all directory

	// load s3 creds
	if err := godotenv.Load(".env"); err != nil {
		log.Fatalf("Error loading .env file")
	}

	// s3 session
	s3.S3Session()

	// Call Download func
	s3.DownloadObjectsFromS3()
}
