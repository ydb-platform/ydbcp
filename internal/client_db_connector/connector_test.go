package client_db_connector

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Export"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Import"
	"ydbcp/internal/types"
)

type ObjectPath struct {
	Bucket    string
	KeyPrefix string
}

type MockClientDbConnector struct {
	storage    map[ObjectPath]bool
	operations map[string]types.YdbOperationInfo
}

func NewMockClientDbConnector() *MockClientDbConnector {
	return &MockClientDbConnector{
		storage:    make(map[ObjectPath]bool),
		operations: make(map[string]types.YdbOperationInfo),
	}
}

func (m *MockClientDbConnector) ExportToS3(_ context.Context, _ types.YdbConnectionParams, s3Settings *Ydb_Export.ExportToS3Settings) (string, error) {
	objects := make([]ObjectPath, 0)
	for _, item := range s3Settings.Items {
		objectPath := ObjectPath{Bucket: s3Settings.Bucket, KeyPrefix: item.DestinationPrefix}
		if m.storage[objectPath] {
			return "", fmt.Errorf("object %v already exist", objectPath)
		}

		objects = append(objects, objectPath)
	}

	for _, object := range objects {
		m.storage[object] = true
	}

	newOp := types.YdbOperationInfo{
		Id:     uuid.NewString(),
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
	}

	m.operations[newOp.Id] = newOp
	return newOp.Id, nil
}

func (m *MockClientDbConnector) ImportFromS3(_ context.Context, _ types.YdbConnectionParams, s3Settings *Ydb_Import.ImportFromS3Settings) (string, error) {
	for _, item := range s3Settings.Items {
		objectPath := ObjectPath{Bucket: s3Settings.Bucket, KeyPrefix: item.SourcePrefix}
		if !m.storage[objectPath] {
			return "", fmt.Errorf("object %v doesn't exist", objectPath)
		}
	}

	newOp := types.YdbOperationInfo{
		Id:     uuid.NewString(),
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
	}
	m.operations[newOp.Id] = newOp
	return newOp.Id, nil
}

func (m *MockClientDbConnector) GetOperationStatus(_ context.Context, _ types.YdbConnectionParams, operationId string) (types.YdbOperationInfo, error) {
	op, exist := m.operations[operationId]
	if !exist {
		return types.YdbOperationInfo{}, fmt.Errorf("operation %s doesn't exist", operationId)
	}

	return op, nil
}
