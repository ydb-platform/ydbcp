package client_db_connector

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
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

func (m *MockClientDbConnector) ExportToS3(_ context.Context, settings types.ExportToS3Settings) (string, error) {
	objectPath := ObjectPath{Bucket: settings.S3.Bucket, KeyPrefix: settings.DestinationPrefix}
	if m.storage[objectPath] {
		return "", fmt.Errorf("object %v already exist", objectPath)
	}

	m.storage[objectPath] = true
	newOp := types.YdbOperationInfo{
		Id:     uuid.NewString(),
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
	}

	m.operations[newOp.Id] = newOp
	return newOp.Id, nil
}

func (m *MockClientDbConnector) ImportFromS3(_ context.Context, settings types.ImportFromS3Settings) (string, error) {
	objectPath := ObjectPath{Bucket: settings.S3.Bucket, KeyPrefix: settings.SourcePrefix}
	if !m.storage[objectPath] {
		return "", fmt.Errorf("object %v doesn't exist", objectPath)
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
