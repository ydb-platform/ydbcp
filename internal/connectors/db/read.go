package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	yt "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"ydbcp/internal/connectors/db/internal/queries"
	"ydbcp/internal/types"
)

func stringFilter(field string, values ...string) queries.QueryFilter {
	f := queries.QueryFilter{Field: field}
	for _, v := range values {
		f.Values = append(f.Values, yt.StringValueFromString(v))
	}
	return f
}

func listOptions(container, databaseMask string, dates TimeRange, page *Page) []queries.ReadTableQueryOption {
	var filters []queries.QueryFilter
	if container != "" {
		filters = append(filters, stringFilter("container_id", container))
	}
	if databaseMask != "" {
		f := stringFilter("database", databaseMask)
		f.IsLike = true
		filters = append(filters, f)
	}
	for _, bound := range []struct {
		value *time.Time
		op    string
	}{{dates.From, ">="}, {dates.To, "<="}} {
		if bound.value != nil {
			filters = append(filters, queries.QueryFilter{Field: "created_at", Operator: bound.op, Values: []yt.Value{yt.TimestampValueFromTime(*bound.value)}})
		}
	}
	opts := []queries.ReadTableQueryOption{queries.WithQueryFilters(filters...)}
	if page != nil {
		opts = append(opts, queries.WithPageSpec(queries.PageSpec{Limit: page.Limit, Offset: page.Offset}))
	}
	return opts
}

func backupOrder(order *BackupOrder) (queries.OrderSpec, error) {
	if order == nil {
		return queries.OrderSpec{Field: "created_at", Desc: true}, nil
	}
	var field string
	switch order.Field {
	case BackupOrderCreatedAt:
		field = "created_at"
	case BackupOrderDatabaseName:
		field = "database"
	case BackupOrderStatus:
		field = "status"
	case BackupOrderExpireAt:
		field = "expire_at"
	case BackupOrderCompletedAt:
		field = "completed_at"
	default:
		return queries.OrderSpec{}, fmt.Errorf("invalid backup order field: %d", order.Field)
	}
	return queries.OrderSpec{Field: field, Desc: order.Desc}, nil
}

func backupQuery(f BackupFilter) (queries.ReadTableQuery, error) {
	order, err := backupOrder(f.Order)
	if err != nil {
		return nil, err
	}
	opts := listOptions(f.ContainerID, f.DatabaseNameMask, f.CreatedAt, f.Page)
	opts = append(opts, queries.WithTableName("Backups"), queries.WithOrderBy(order))
	if len(f.Statuses) > 0 {
		opts = append(opts, queries.WithQueryFilters(stringFilter("status", f.Statuses...)))
	}
	return queries.NewReadTableQuery(opts...), nil
}

func (d *YdbConnector) ListBackups(ctx context.Context, f BackupFilter) ([]*types.Backup, error) {
	q, err := backupQuery(f)
	if err != nil {
		return nil, err
	}
	return selectRows(ctx, d, q, readBackupFromResultSet)
}

func operationQuery(f OperationFilter) queries.ReadTableQuery {
	opts := listOptions(f.ContainerID, f.DatabaseNameMask, f.CreatedAt, f.Page)
	opts = append(opts, queries.WithTableName("Operations"), queries.WithOrderBy(queries.OrderSpec{Field: "created_at", Desc: true}))
	if len(f.Types) > 0 {
		values := make([]string, len(f.Types))
		for i, t := range f.Types {
			values[i] = t.String()
		}
		opts = append(opts, queries.WithQueryFilters(stringFilter("type", values...)))
	}
	return queries.NewReadTableQuery(opts...)
}

func (d *YdbConnector) ListOperations(ctx context.Context, f OperationFilter) ([]types.Operation, error) {
	return selectRows(ctx, d, operationQuery(f), readOperationFromResultSet)
}

func scheduleQuery(f ScheduleFilter, withInfo bool) queries.ReadTableQuery {
	opts := listOptions(f.ContainerID, f.DatabaseNameMask, TimeRange{}, f.Page)
	if withInfo {
		opts = append(opts, queries.WithRawQuery(queries.ListSchedulesQuery))
	} else {
		opts = append(opts, queries.WithTableName("BackupSchedules"))
	}
	opts = append(opts, queries.WithOrderBy(queries.OrderSpec{Field: "created_at", Desc: true}))
	if f.DatabaseName != "" {
		opts = append(opts, queries.WithQueryFilters(stringFilter("database", f.DatabaseName)))
	}
	if len(f.Statuses) > 0 {
		opts = append(opts, queries.WithQueryFilters(stringFilter("status", f.Statuses...)))
	}
	return queries.NewReadTableQuery(opts...)
}

func (d *YdbConnector) ListSchedules(ctx context.Context, f ScheduleFilter) ([]*types.BackupSchedule, error) {
	return d.selectSchedules(ctx, scheduleQuery(f, false), false)
}

func (d *YdbConnector) ListSchedulesWithBackupInfo(ctx context.Context, f ScheduleFilter) ([]*types.BackupSchedule, error) {
	return d.selectSchedules(ctx, scheduleQuery(f, true), true)
}

func (d *YdbConnector) selectSchedules(ctx context.Context, q queries.ReadTableQuery, withInfo bool) ([]*types.BackupSchedule, error) {
	return selectRows(ctx, d, q, func(row query.Row) (*types.BackupSchedule, error) {
		return readBackupScheduleFromResultSet(row, withInfo)
	})
}

func byID(tableName, id string) queries.ReadTableQuery {
	return queries.NewReadTableQuery(queries.WithTableName(tableName), queries.WithQueryFilters(stringFilter("id", id)))
}

func one[T any](items []T, err error) (T, error) {
	var zero T
	if err != nil {
		return zero, err
	}
	if len(items) == 0 {
		return zero, ErrNotFound
	}
	if len(items) != 1 {
		return zero, fmt.Errorf("expected one metadata entity, got %d", len(items))
	}
	return items[0], nil
}

func (d *YdbConnector) GetBackup(ctx context.Context, id string) (*types.Backup, error) {
	return one(selectRows(ctx, d, byID("Backups", id), readBackupFromResultSet))
}

func (d *YdbConnector) GetOperation(ctx context.Context, id string) (types.Operation, error) {
	return one(selectRows(ctx, d, byID("Operations", id), readOperationFromResultSet))
}

func (d *YdbConnector) GetSchedule(ctx context.Context, id string) (*types.BackupSchedule, error) {
	return one(d.selectSchedules(ctx, byID("BackupSchedules", id), false))
}

func (d *YdbConnector) GetScheduleWithBackupInfo(ctx context.Context, id string) (*types.BackupSchedule, error) {
	q := queries.NewReadTableQuery(queries.WithRawQuery(queries.GetScheduleQuery),
		queries.WithParameters(table.ValueParam("$schedule_id", yt.StringValueFromString(id))))
	return one(d.selectSchedules(ctx, q, true))
}

// ListChildOperations returns attempts in ascending creation order.
func (d *YdbConnector) ListChildOperations(ctx context.Context, parentID string) ([]types.Operation, error) {
	return selectRows(ctx, d, queries.NewReadTableQuery(
		queries.WithTableName("Operations"),
		queries.WithIndex("idx_p"),
		queries.WithQueryFilters(stringFilter("parent_operation_id", parentID)),
		queries.WithOrderBy(queries.OrderSpec{Field: "created_at"}),
	), readOperationFromResultSet)
}

func (d *YdbConnector) ActiveOperations(ctx context.Context) ([]types.Operation, error) {
	return selectRows(ctx, d, queries.NewReadTableQuery(
		queries.WithTableName("Operations"),
		queries.WithQueryFilters(stringFilter("status", types.OperationStatePending.String(),
			types.OperationStateRunning.String(), types.OperationStateCancelling.String(), types.OperationStateStartCancelling.String())),
	), readOperationFromResultSet)
}

// ListExpiredBackups uses database time, as did the original TTL query.
// A zero limit returns no candidates.
func (d *YdbConnector) ListExpiredBackups(ctx context.Context, limit uint64) ([]*types.Backup, error) {
	if limit == 0 {
		return nil, ctx.Err()
	}
	return selectRows(ctx, d, queries.NewReadTableQuery(
		queries.WithRawQuery(queries.GetBackupsToDeleteQuery),
		queries.WithPageSpec(queries.PageSpec{Limit: limit}),
	), readBackupFromResultSet)
}
