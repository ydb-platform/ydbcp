package db

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/connectors/db/internal/queries"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
)

func (c Changes) empty() bool {
	return len(c.CreateBackups)+len(c.UpdateBackups)+len(c.CreateOperations)+len(c.UpdateOperations)+len(c.CreateSchedules)+len(c.UpdateSchedules) == 0
}

func (c Changes) validate() error {
	for _, group := range [][]types.Backup{c.CreateBackups, c.UpdateBackups} {
		for _, b := range group {
			if b.ID == "" {
				return fmt.Errorf("backup ID is required")
			}
		}
	}
	for _, group := range [][]types.Operation{c.CreateOperations, c.UpdateOperations} {
		for _, op := range group {
			if nilOperation(op) {
				return fmt.Errorf("operation is required")
			}
			if op.GetID() == "" {
				return fmt.Errorf("operation ID is required")
			}
		}
	}
	for _, op := range c.CreateOperations {
		switch operation := op.(type) {
		case *types.TakeBackupOperation, *types.RestoreBackupOperation, *types.DeleteBackupOperation:
		case *types.TakeBackupWithRetryOperation:
			if config := operation.RetryConfig; config != nil {
				switch retry := config.Retries.(type) {
				case *pb.RetryConfig_Count:
					if retry == nil {
						return fmt.Errorf("retry count is required")
					}
				case *pb.RetryConfig_MaxBackoff:
					if retry == nil {
						return fmt.Errorf("retry backoff is required")
					}
				default:
					return fmt.Errorf("retry configuration must select count or max backoff")
				}
			}
		default:
			return fmt.Errorf("unsupported operation type %T", op)
		}
	}
	for _, group := range [][]types.BackupSchedule{c.CreateSchedules, c.UpdateSchedules} {
		for _, s := range group {
			if s.ID == "" {
				return fmt.Errorf("schedule ID is required")
			}
			if s.ScheduleSettings == nil || s.ScheduleSettings.SchedulePattern == nil {
				return fmt.Errorf("schedule pattern is required")
			}
		}
	}
	return nil
}

func writeQuery(c Changes) queries.WriteTableQuery {
	q := queries.NewWriteTableQuery()
	for _, b := range c.CreateBackups {
		q.WithCreateBackup(b)
	}
	for _, op := range c.CreateOperations {
		q.WithCreateOperation(op)
	}
	for _, s := range c.CreateSchedules {
		q.WithCreateBackupSchedule(s)
	}
	for _, b := range c.UpdateBackups {
		q.WithUpdateBackup(b)
	}
	for _, op := range c.UpdateOperations {
		q.WithUpdateOperation(op)
	}
	for _, s := range c.UpdateSchedules {
		q.WithUpdateBackupSchedule(s)
	}
	return q
}

func (d *YdbConnector) Apply(ctx context.Context, c Changes) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.validate(); err != nil {
		return err
	}
	if c.empty() {
		return nil
	}
	q, err := writeQuery(c).FormatQuery(ctx)
	if err != nil {
		return err
	}
	err = d.client.Do(ctx, func(ctx context.Context, s query.Session) error {
		return s.Exec(ctx, q.QueryText, query.WithParameters(q.QueryParams), query.WithTxControl(writeTx))
	})
	if err != nil {
		reportDBError(ctx, err)
		return err
	}
	reportCreatedOperations(c)
	return nil
}

func reportCreatedOperations(c Changes) {
	for _, op := range c.CreateOperations {
		metrics.GlobalMetricsRegistry.IncOperationsStartedCounter(op)
	}
}

func updateOperation(ctx context.Context, store DBConnector, op types.Operation) error {
	if nilOperation(op) {
		return fmt.Errorf("operation is required")
	}
	if op.GetAudit() != nil && op.GetAudit().CompletedAt != nil {
		op.SetUpdatedAt(op.GetAudit().CompletedAt)
	} else {
		op.SetUpdatedAt(timestamppb.Now())
	}
	return store.Apply(ctx, Changes{UpdateOperations: []types.Operation{op}})
}

func (d *YdbConnector) UpdateOperation(ctx context.Context, op types.Operation) error {
	return updateOperation(ctx, d, op)
}

// CreateOperation preserves a supplied ID, generating it only when absent.
func createOperation(ctx context.Context, store DBConnector, op types.Operation) (string, error) {
	if nilOperation(op) {
		return "", fmt.Errorf("operation is required")
	}
	if op.GetID() == "" {
		op.SetID(types.GenerateObjectID())
	}
	if err := store.Apply(ctx, Changes{CreateOperations: []types.Operation{op}}); err != nil {
		return "", err
	}
	return op.GetID(), nil
}

func (d *YdbConnector) CreateOperation(ctx context.Context, op types.Operation) (string, error) {
	return createOperation(ctx, d, op)
}

func createBackup(ctx context.Context, store DBConnector, b types.Backup) (string, error) {
	if b.ID == "" {
		b.ID = types.GenerateObjectID()
	}
	if err := store.Apply(ctx, Changes{CreateBackups: []types.Backup{b}}); err != nil {
		return "", err
	}
	return b.ID, nil
}

func (d *YdbConnector) CreateBackup(ctx context.Context, b types.Backup) (string, error) {
	return createBackup(ctx, d, b)
}

func nilOperation(op types.Operation) bool {
	return op == nil || (reflect.ValueOf(op).Kind() == reflect.Ptr && reflect.ValueOf(op).IsNil())
}
