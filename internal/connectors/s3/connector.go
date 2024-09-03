package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"ydbcp/internal/config"
)

type S3Connector interface {
	ListObjects(pathPrefix string, bucket string) ([]string, error)
	GetSize(pathPrefix string, bucket string) (int64, error)
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

func (c *ClientS3Connector) ListObjects(pathPrefix string, bucket string) ([]string, error) {
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
			}

			return true
		},
	)

	if err != nil {
		return nil, err
	}

	return *objectsPtr, nil
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
