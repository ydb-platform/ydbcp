package s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
)

type MockS3Connector struct {
	storage map[string][]*s3.Object
}

func NewMockS3Connector(storage map[string][]*s3.Object) *MockS3Connector {
	return &MockS3Connector{
		storage: storage,
	}
}

func (m *MockS3Connector) ListObjects(pathPrefix string, _ string) ([]string, error) {
	objects := make([]string, 0)

	if content, ok := m.storage[pathPrefix]; ok {
		for _, object := range content {
			if object.Key == nil {
				objects = append(objects, *object.Key)
			}
		}
	}

	return objects, nil
}

func (m *MockS3Connector) GetSize(pathPrefix string, _ string) (int64, error) {
	if content, ok := m.storage[pathPrefix]; ok {
		var size int64
		for _, object := range content {
			if object.Size != nil {
				size += *object.Size
			}
		}

		return size, nil
	}

	return 0, fmt.Errorf("objects not found, path: %s", pathPrefix)
}

func (m *MockS3Connector) DeleteObjects(objects []string, _ string) error {
	for _, object := range objects {
		delete(m.storage, object)
	}

	return nil
}
