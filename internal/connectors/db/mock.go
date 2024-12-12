package db

import (
	"context"
	"fmt"
	"sync"
	"ydbcp/internal/metrics"

	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type MockDBConnector struct {
	guard           sync.Mutex
	operations      map[string]types.Operation
	backups         map[string]types.Backup
	backupSchedules map[string]types.BackupSchedule
	operationIDs    *[]string
}

type Option func(*MockDBConnector)

func NewMockDBConnector(options ...Option) *MockDBConnector {
	connector := &MockDBConnector{
		operations:      make(map[string]types.Operation),
		backups:         make(map[string]types.Backup),
		backupSchedules: make(map[string]types.BackupSchedule),
	}
	for _, opt := range options {
		opt(connector)
	}
	return connector
}

func WithOperations(operations map[string]types.Operation) Option {
	return func(c *MockDBConnector) {
		c.operations = operations
	}
}

func WithBackups(backups map[string]types.Backup) Option {
	return func(c *MockDBConnector) {
		c.backups = backups
	}
}

func WithBackupSchedules(backupSchedules map[string]types.BackupSchedule) Option {
	return func(c *MockDBConnector) {
		c.backupSchedules = backupSchedules
	}
}

func (c *MockDBConnector) SelectBackups(
	_ context.Context, _ queries.ReadTableQuery,
) ([]*types.Backup, error) {
	c.guard.Lock()
	defer c.guard.Unlock()

	backups := make([]*types.Backup, 0, len(c.backups))
	for _, backup := range c.backups {
		backups = append(backups, &backup)
	}
	return backups, nil
}

func (c *MockDBConnector) SelectBackupSchedules(
	_ context.Context, _ queries.ReadTableQuery,
) ([]*types.BackupSchedule, error) {
	c.guard.Lock()
	defer c.guard.Unlock()

	schedules := make([]*types.BackupSchedule, 0, len(c.backupSchedules))
	for _, schedule := range c.backupSchedules {
		schedules = append(schedules, &schedule)
	}
	return schedules, nil
}

func (c *MockDBConnector) SelectBackupSchedulesWithRPOInfo(
	a context.Context, b queries.ReadTableQuery,
) ([]*types.BackupSchedule, error) {
	return c.SelectBackupSchedules(a, b)
}

func (c *MockDBConnector) SelectBackupsByStatus(
	_ context.Context, _ string,
) ([]*types.Backup, error) {
	c.guard.Lock()
	defer c.guard.Unlock()

	backups := make([]*types.Backup, 0, len(c.backups))
	for _, backup := range c.backups {
		backups = append(backups, &backup)
	}
	return backups, nil
}

func (c *MockDBConnector) UpdateBackup(
	_ context.Context, id string, backupStatus string,
) error {
	c.guard.Lock()
	defer c.guard.Unlock()

	if _, ok := c.backups[id]; !ok {
		return fmt.Errorf("no backup found for id %v", id)
	}
	backup := c.backups[id]
	backup.Status = backupStatus
	c.backups[id] = backup
	return nil
}

func (c *MockDBConnector) Close(_ context.Context) {}
func (c *MockDBConnector) GetTableClient() table.Client {
	return nil
}

func (c *MockDBConnector) CreateBackup(_ context.Context, backup types.Backup) (string, error) {
	c.guard.Lock()
	defer c.guard.Unlock()

	var id string
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
	c.guard.Lock()
	defer c.guard.Unlock()

	operations := make([]types.Operation, 0, len(c.operations))
	for _, op := range c.operations {
		if types.IsActive(op) {
			operations = append(operations, op.Copy())
		}
	}
	return operations, nil
}

func (c *MockDBConnector) UpdateOperation(
	_ context.Context, op types.Operation,
) error {
	c.guard.Lock()
	defer c.guard.Unlock()

	if _, exist := c.operations[op.GetID()]; !exist {
		return fmt.Errorf("update nonexistent operation %s", types.OperationToString(op))
	}
	c.operations[op.GetID()] = op.Copy()
	return nil
}

func (c *MockDBConnector) CreateOperation(
	_ context.Context, op types.Operation,
) (string, error) {
	c.guard.Lock()
	defer c.guard.Unlock()

	var id string
	for {
		id = types.GenerateObjectID()
		if _, exist := c.operations[id]; !exist {
			break
		}
	}
	op.SetID(id)
	c.operations[id] = op.Copy()
	return id, nil
}

func (c *MockDBConnector) GetOperation(
	_ context.Context, operationID string,
) (types.Operation, error) {
	c.guard.Lock()
	defer c.guard.Unlock()

	if op, exist := c.operations[operationID]; exist {
		return op.Copy(), nil
	}
	return nil, fmt.Errorf("operation not found, id %s", operationID)
}

func (c *MockDBConnector) GetBackup(
	_ context.Context, backupID string,
) (types.Backup, error) {
	c.guard.Lock()
	defer c.guard.Unlock()

	if backup, exist := c.backups[backupID]; exist {
		return backup, nil
	}
	return types.Backup{}, fmt.Errorf("backup not found, id %s", backupID)
}

func (c *MockDBConnector) GetSchedule(
	_ context.Context, scheduleID string,
) (types.BackupSchedule, error) {
	c.guard.Lock()
	defer c.guard.Unlock()

	if schedule, exist := c.backupSchedules[scheduleID]; exist {
		return schedule, nil
	}
	return types.BackupSchedule{}, fmt.Errorf("backupSchedule not found, id %s", scheduleID)
}

func (c *MockDBConnector) SetOperationsIDSelector(ids []string) {
	c.guard.Lock()
	defer c.guard.Unlock()

	c.operationIDs = &ids
}

func (c *MockDBConnector) SelectOperations(
	_ context.Context, _ queries.ReadTableQuery,
) ([]types.Operation, error) {
	c.guard.Lock()
	defer c.guard.Unlock()
	if c.operationIDs != nil {
		res := make([]types.Operation, 0, len(*c.operationIDs))
		for _, id := range *c.operationIDs {
			res = append(res, c.operations[id].Copy())
		}
		c.operationIDs = nil
		return res, nil
	}
	res := make([]types.Operation, 0, len(c.operations))
	for _, v := range c.operations {
		res = append(res, v.Copy())
	}
	return res, nil
}

func (c *MockDBConnector) ExecuteUpsert(_ context.Context, queryBuilder queries.WriteTableQuery) error {
	c.guard.Lock()
	defer c.guard.Unlock()

	queryBuilderMock := queryBuilder.(*queries.WriteTableQueryMock)
	if queryBuilderMock.Operations != nil {
		for _, op := range queryBuilderMock.Operations {
			c.operations[op.GetID()] = op
			metrics.GlobalMetricsRegistry.IncOperationsStartedCounter(op)
		}
	}
	if queryBuilderMock.Backup != nil {
		c.backups[queryBuilderMock.Backup.ID] = *queryBuilderMock.Backup
	}
	if queryBuilderMock.BackupSchedule != nil {
		c.backupSchedules[queryBuilderMock.BackupSchedule.ID] = *queryBuilderMock.BackupSchedule
	}
	return nil
}
