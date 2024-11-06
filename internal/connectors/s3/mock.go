package s3

import (
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
)

type Bucket map[string]*s3.Object

type MockS3Connector struct {
	storage map[string]Bucket
}

func NewMockS3Connector(storage map[string]Bucket) *MockS3Connector {
	return &MockS3Connector{
		storage: storage,
	}
}

func (m *MockS3Connector) ListObjects(pathPrefix string, bucketName string) ([]string, int64, error) {
	objects := make([]string, 0)
	var size int64

	if bucket, ok := m.storage[bucketName]; ok {
		for key, object := range bucket {
			if strings.HasPrefix(key, pathPrefix) {
				objects = append(objects, key)

				if object.Size != nil {
					size += *object.Size
				}
			}
		}
	}

	return objects, size, nil
}

func (m *MockS3Connector) GetSize(pathPrefix string, bucketName string) (int64, error) {
	var size int64

	if bucket, ok := m.storage[bucketName]; ok {
		for key, object := range bucket {
			if strings.HasPrefix(key, pathPrefix) {
				if object.Size != nil {
					size += *object.Size
				}
			}
		}
	}

	return size, nil
}

func (m *MockS3Connector) DeleteObjects(objects []string, bucketName string) error {
	if bucket, ok := m.storage[bucketName]; ok {
		for _, key := range objects {
			delete(bucket, key)
		}
	}

	return nil
}
