package s3

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Bucket map[string][]byte

type MockS3Connector struct {
	storage map[string]Bucket
}

func (m *MockS3Connector) ListObjectsPages(input *s3.ListObjectsInput, fn func(*s3.ListObjectsOutput, bool) bool) error {
	objects, _, err := m.ListObjects(*input.Prefix, *input.Bucket)
	if err != nil {
		return err
	}

	var s3objs []*s3.Object
	for i := 0; i < len(objects); i++ {
		s3objs = append(s3objs, &s3.Object{
			Key: aws.String(objects[i]),
		})
	}

	_ = fn(&s3.ListObjectsOutput{Contents: s3objs}, true)
	return nil
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
		for key, data := range bucket {
			if strings.HasPrefix(key, pathPrefix) {
				objects = append(objects, key)
				size += int64(len(data))
			}
		}
	}

	return objects, size, nil
}

func (m *MockS3Connector) GetSize(pathPrefix string, bucketName string) (int64, error) {
	var size int64

	if bucket, ok := m.storage[bucketName]; ok {
		for key, data := range bucket {
			if strings.HasPrefix(key, pathPrefix) {
				size += int64(len(data))
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

func (m *MockS3Connector) PutObject(key string, bucketName string, data []byte) error {
	if _, ok := m.storage[bucketName]; !ok {
		m.storage[bucketName] = make(Bucket)
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	m.storage[bucketName][key] = dataCopy

	return nil
}

func (m *MockS3Connector) GetObject(key string, bucketName string) ([]byte, error) {
	bucket, ok := m.storage[bucketName]
	if !ok {
		return nil, fmt.Errorf("bucket %s not found", bucketName)
	}

	data, ok := bucket[key]
	if !ok {
		return nil, fmt.Errorf("object %s not found in bucket %s", key, bucketName)
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return dataCopy, nil
}
