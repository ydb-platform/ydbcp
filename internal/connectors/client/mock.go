package client

import (
	"context"
	"fmt"
	"path"
	"ydbcp/internal/types"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Import"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type ObjectPath struct {
	Bucket    string
	KeyPrefix string
}

type MockClientConnector struct {
	storage    map[ObjectPath]bool
	operations map[string]*Ydb_Operations.Operation
}

type Option func(*MockClientConnector)

func NewMockClientConnector(options ...Option) *MockClientConnector {
	connector := &MockClientConnector{
		storage:    make(map[ObjectPath]bool),
		operations: make(map[string]*Ydb_Operations.Operation),
	}
	for _, opt := range options {
		opt(connector)
	}
	return connector

}

func WithOperations(operations map[string]*Ydb_Operations.Operation) Option {
	return func(c *MockClientConnector) {
		c.operations = operations
	}
}

func (m *MockClientConnector) Open(_ context.Context, _ string) (*ydb.Driver, error) {
	return nil, nil
}

func (m *MockClientConnector) Close(_ context.Context, _ *ydb.Driver) error {
	return nil
}

func (m *MockClientConnector) ExportToS3(_ context.Context, _ *ydb.Driver, s3Settings types.ExportSettings) (string, error) {
	objects := make([]ObjectPath, 0)
	for _, source := range s3Settings.SourcePaths {
		objectPath := ObjectPath{Bucket: s3Settings.Bucket, KeyPrefix: path.Join(s3Settings.DestinationPrefix, source)}
		if m.storage[objectPath] {
			return "", fmt.Errorf("object %v already exist", objectPath)
		}

		objects = append(objects, objectPath)
	}

	for _, object := range objects {
		m.storage[object] = true
	}

	newOp := &Ydb_Operations.Operation{
		Id:     uuid.NewString(),
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
	}

	m.operations[newOp.Id] = newOp
	return newOp.Id, nil
}

func (m *MockClientConnector) ImportFromS3(_ context.Context, _ *ydb.Driver, s3Settings *Ydb_Import.ImportFromS3Settings) (string, error) {
	for _, item := range s3Settings.Items {
		objectPath := ObjectPath{Bucket: s3Settings.Bucket, KeyPrefix: item.SourcePrefix}
		if !m.storage[objectPath] {
			return "", fmt.Errorf("object %v doesn't exist", objectPath)
		}
	}

	newOp := &Ydb_Operations.Operation{
		Id:     uuid.NewString(),
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
	}
	m.operations[newOp.Id] = newOp
	return newOp.Id, nil
}

func (m *MockClientConnector) GetOperationStatus(_ context.Context, _ *ydb.Driver, operationId string) (*Ydb_Operations.GetOperationResponse, error) {
	op, exist := m.operations[operationId]
	if !exist {
		issues := make([]*Ydb_Issue.IssueMessage, 1)
		issues[0] = &Ydb_Issue.IssueMessage{
			Message: "operation not found",
		}

		return &Ydb_Operations.GetOperationResponse{
			Operation: &Ydb_Operations.Operation{
				Status: Ydb.StatusIds_NOT_FOUND,
				Issues: issues,
			},
		}, nil
	}

	return &Ydb_Operations.GetOperationResponse{
		Operation: op,
	}, nil
}

func (m *MockClientConnector) ForgetOperation(_ context.Context, _ *ydb.Driver, operationId string) (*Ydb_Operations.ForgetOperationResponse, error) {
	_, exist := m.operations[operationId]
	if !exist {
		issues := make([]*Ydb_Issue.IssueMessage, 1)
		issues[0] = &Ydb_Issue.IssueMessage{
			Message: "operation not found",
		}

		return &Ydb_Operations.ForgetOperationResponse{
			Status: Ydb.StatusIds_NOT_FOUND,
			Issues: issues,
		}, nil
	}

	delete(m.operations, operationId)
	return &Ydb_Operations.ForgetOperationResponse{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil
}

func (m *MockClientConnector) CancelOperation(_ context.Context, _ *ydb.Driver, operationId string) (*Ydb_Operations.CancelOperationResponse, error) {
	op, exist := m.operations[operationId]
	if !exist {
		issues := make([]*Ydb_Issue.IssueMessage, 1)
		issues[0] = &Ydb_Issue.IssueMessage{
			Message: "operation not found",
		}

		return &Ydb_Operations.CancelOperationResponse{
			Status: Ydb.StatusIds_NOT_FOUND,
			Issues: issues,
		}, nil
	}

	op.Status = Ydb.StatusIds_CANCELLED
	return &Ydb_Operations.CancelOperationResponse{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil
}
