package db

import (
	"context"
	"errors"
	"fmt"
	"ydbcp/internal/types"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type MockDBConnector struct {
	operations map[types.ObjectID]types.Operation
	backups    map[types.ObjectID]types.Backup
}

type Option func(*MockDBConnector)

func NewMockDBConnector(options ...Option) *MockDBConnector {
	connector := &MockDBConnector{
		operations: make(map[types.ObjectID]types.Operation),
		backups:    make(map[types.ObjectID]types.Backup),
	}
	for _, opt := range options {
		opt(connector)
	}
	return connector
}

func WithOperations(operations map[types.ObjectID]types.Operation) Option {
	return func(c *MockDBConnector) {
		c.operations = operations
	}
}

func WithBackups(backups map[types.ObjectID]types.Backup) Option {
	return func(c *MockDBConnector) {
		c.backups = backups
	}
}

func (c *MockDBConnector) SelectBackups(
	ctx context.Context, backupStatus string,
) ([]*types.Backup, error) {
	backups := make([]*types.Backup, 0, len(c.backups))
	for _, backup := range c.backups {
		backups = append(backups, &backup)
	}
	return backups, nil
}

func (c *MockDBConnector) UpdateBackup(
	ctx context.Context, id types.ObjectID, backupStatus string,
) error {
	if _, ok := c.backups[id]; !ok {
		return errors.New(fmt.Sprintf("no backup found for id %v", id))
	}
	backup := c.backups[id]
	backup.Status = backupStatus
	c.backups[id] = backup
	return nil
}

func (c *MockDBConnector) Close() {}
func (c *MockDBConnector) GetTableClient() table.Client {
	return nil
}

func (c *MockDBConnector) CreateBackup(ctx context.Context, backup types.Backup) (types.ObjectID, error) {
	var id types.ObjectID
	for {
		id = types.GenerateObjectID()
		if _, exist := c.backups[id]; !exist {
			break
		}
	}
	backup.ID = id
	c.backups[id] = backup
	return id, nil
}

func (c *MockDBConnector) ActiveOperations(_ context.Context) (
	[]types.Operation, error,
) {
	operations := make([]types.Operation, 0, len(c.operations))
	for _, op := range c.operations {
		if types.IsActive(op) {
			operations = append(operations, op)
		}
	}
	return operations, nil
}

func (c *MockDBConnector) UpdateOperation(
	_ context.Context, op types.Operation,
) error {
	if _, exist := c.operations[op.GetId()]; !exist {
		return fmt.Errorf(
			"update nonexistent operation %s", types.OperationToString(op),
		)
	}
	c.operations[op.GetId()] = op
	return nil
}

func (c *MockDBConnector) CreateOperation(
	_ context.Context, op types.Operation,
) (types.ObjectID, error) {
	var id types.ObjectID
	for {
		id = types.GenerateObjectID()
		if _, exist := c.operations[id]; !exist {
			break
		}
	}
	op.SetId(id)
	c.operations[id] = op
	return id, nil
}

func (c *MockDBConnector) GetOperation(
	_ context.Context, operationID types.ObjectID,
) (types.Operation, error) {
	if op, exist := c.operations[operationID]; exist {
		return op, nil
	}
	return &types.GenericOperation{}, fmt.Errorf(
		"operation not found, id %s", operationID.String(),
	)
}
