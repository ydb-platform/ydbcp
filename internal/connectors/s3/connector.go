package s3

import (
	"fmt"
	"strings"

	"ydbcp/internal/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Connector interface {
	ListObjects(pathPrefix string, bucket string) ([]string, int64, error)
	GetSize(pathPrefix string, bucket string) (int64, error)
	DeleteObjects(keys []string, bucket string) error
}

type ClientS3Connector struct {
	s3 *s3.S3
}

func NewS3Connector(config config.S3Config) (*ClientS3Connector, error) {
	accessKey, err := config.AccessKey()
	if err != nil {
		return nil, fmt.Errorf("can't get S3AccessKey: %v", err)
	}
	secretKey, err := config.SecretKey()
	if err != nil {
		return nil, fmt.Errorf("can't get S3SecretKey: %v", err)
	}

	s, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("can't create S3 session: %v", err)
	}

	s3Client := s3.New(s,
		&aws.Config{
			Region:           &config.Region,
			Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
			Endpoint:         &config.Endpoint,
			S3ForcePathStyle: &config.S3ForcePathStyle,
		},
	)

	return &ClientS3Connector{s3: s3Client}, nil
}

func (c *ClientS3Connector) ListObjects(pathPrefix string, bucket string) ([]string, int64, error) {
	var size int64
	objects := make([]string, 0)
	objectsPtr := &objects

	if !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix = pathPrefix + "/"
	}

	err := c.s3.ListObjectsPages(
		&s3.ListObjectsInput{
			Bucket: &bucket,
			Prefix: &pathPrefix,
		},
		func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
			for _, object := range p.Contents {
				*objectsPtr = append(*objectsPtr, *object.Key)
				if object.Size != nil {
					size += *object.Size
				}
			}

			return true
		},
	)

	if err != nil {
		return nil, 0, err
	}

	return *objectsPtr, size, nil
}

func (c *ClientS3Connector) GetSize(pathPrefix string, bucket string) (int64, error) {
	var size int64

	if !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix = pathPrefix + "/"
	}

	err := c.s3.ListObjectsPages(
		&s3.ListObjectsInput{
			Bucket: &bucket,
			Prefix: &pathPrefix,
		},
		func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
			for _, object := range p.Contents {
				if object.Size != nil {
					size += *object.Size
				}
			}

			return true
		},
	)

	if err != nil {
		return 0, err
	}

	return size, nil
}

func (c *ClientS3Connector) DeleteObjects(keys []string, bucket string) error {
	// S3 API allows to delete up to 1000 objects per request
	const DeleteObjectsListSizeLimit = 1000
	for begin := 0; begin < len(keys); begin += DeleteObjectsListSizeLimit {
		var end int

		if begin+DeleteObjectsListSizeLimit < len(keys) {
			end = begin + DeleteObjectsListSizeLimit
		} else {
			end = len(keys)
		}

		objectsPtr := make([]*s3.ObjectIdentifier, end-begin)
		for i, object := range keys[begin:end] {
			objectsPtr[i] = &s3.ObjectIdentifier{
				Key: aws.String(object),
			}
		}

		input := s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3.Delete{
				Objects: objectsPtr,
				Quiet:   aws.Bool(true),
			},
		}

		delOut, err := c.s3.DeleteObjects(&input)
		if err != nil {
			return err
		}

		if len(delOut.Errors) > 0 {
			return fmt.Errorf("can't delete objects: %v", delOut.Errors)
		}
	}

	return nil
}
