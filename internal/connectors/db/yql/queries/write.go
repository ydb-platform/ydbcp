package queries

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

type WriteTableQuery interface {
	FormatQuery(ctx context.Context) (*FormatQueryResult, error)
	WithCreateBackup(backup types.Backup) WriteTableQuery
	WithCreateOperation(operation types.Operation) WriteTableQuery
	WithCreateBackupSchedule(schedule types.BackupSchedule) WriteTableQuery
	WithUpdateBackup(backup types.Backup) WriteTableQuery
	WithUpdateOperation(operation types.Operation) WriteTableQuery
	WithUpdateBackupSchedule(schedule types.BackupSchedule) WriteTableQuery
}

type WriteTableQueryImpl struct {
	tableQueries []WriteSingleTableQueryImpl
}

type WriteSingleTableQueryImpl struct {
	index            int
	tableName        string
	upsertFields     []string
	tableQueryParams []table.ParameterOption
	updateParam      *table.ParameterOption
}

type WriteTableQueryImplOption func(*WriteTableQueryImpl)
type WriteQueryBulderFactory func() WriteTableQuery

func (d *WriteSingleTableQueryImpl) AddValueParam(name string, value table_types.Value) {
	d.upsertFields = append(d.upsertFields, name[1:])
	d.tableQueryParams = append(d.tableQueryParams, table.ValueParam(fmt.Sprintf("%s_%d", name, d.index), value))
}

func (d *WriteSingleTableQueryImpl) AddUpdateId(value table_types.Value) {
	updateParamName := fmt.Sprintf("$id_%d", d.index)
	vp := table.ValueParam(updateParamName, value)
	d.updateParam = &vp
}

func (d *WriteSingleTableQueryImpl) GetParamNames() []string {
	res := make([]string, len(d.tableQueryParams))
	for i, p := range d.tableQueryParams {
		res[i] = p.Name()
	}
	return res
}

func BuildCreateOperationQuery(operation types.Operation, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Operations",
	}
	d.AddValueParam("$id", table_types.StringValueFromString(operation.GetID()))
	d.AddValueParam("$type", table_types.StringValueFromString(string(operation.GetType())))
	d.AddValueParam("$status", table_types.StringValueFromString(operation.GetState().String()))
	d.AddValueParam("$message", table_types.StringValueFromString(operation.GetMessage()))
	if operation.GetAudit() != nil {
		d.AddValueParam(
			"$initiated",
			table_types.StringValueFromString(operation.GetAudit().Creator),
		)
		d.AddValueParam(
			"$created_at",
			table_types.TimestampValueFromTime(operation.GetAudit().CreatedAt.AsTime()),
		)
		if operation.GetAudit().CompletedAt != nil {
			d.AddValueParam(
				"completed_at",
				table_types.TimestampValueFromTime(operation.GetAudit().CompletedAt.AsTime()),
			)
		}
	}
	if operation.GetUpdatedAt() != nil {
		d.AddValueParam(
			"$updated_at",
			table_types.TimestampValueFromTime(operation.GetUpdatedAt().AsTime()),
		)
	}

	if operation.GetType() == types.OperationTypeTB {
		tb, ok := operation.(*types.TakeBackupOperation)
		if !ok {
			log.Fatalf("error cast operation to TakeBackupOperation operation_id %s", operation.GetID())
		}

		d.AddValueParam(
			"$container_id", table_types.StringValueFromString(tb.ContainerID),
		)
		d.AddValueParam(
			"$database",
			table_types.StringValueFromString(tb.YdbConnectionParams.DatabaseName),
		)
		d.AddValueParam(
			"$endpoint",
			table_types.StringValueFromString(tb.YdbConnectionParams.Endpoint),
		)
		d.AddValueParam(
			"$backup_id",
			table_types.StringValueFromString(tb.BackupID),
		)
		d.AddValueParam(
			"$operation_id",
			table_types.StringValueFromString(tb.YdbOperationId),
		)
		if len(tb.SourcePaths) > 0 {
			d.AddValueParam("$paths", table_types.StringValueFromString(strings.Join(tb.SourcePaths, ",")))
		}
		if len(tb.SourcePathsToExclude) > 0 {
			d.AddValueParam(
				"$paths_to_exclude",
				table_types.StringValueFromString(strings.Join(tb.SourcePathsToExclude, ",")),
			)
		}
	} else if operation.GetType() == types.OperationTypeRB {
		rb, ok := operation.(*types.RestoreBackupOperation)
		if !ok {
			log.Fatalf("error cast operation to RestoreBackupOperation operation_id %s", operation.GetID())
		}

		d.AddValueParam(
			"$container_id", table_types.StringValueFromString(rb.ContainerID),
		)
		d.AddValueParam(
			"$database",
			table_types.StringValueFromString(rb.YdbConnectionParams.DatabaseName),
		)
		d.AddValueParam(
			"$endpoint",
			table_types.StringValueFromString(rb.YdbConnectionParams.Endpoint),
		)
		d.AddValueParam(
			"$backup_id",
			table_types.StringValueFromString(rb.BackupId),
		)
		d.AddValueParam(
			"$operation_id",
			table_types.StringValueFromString(rb.YdbOperationId),
		)

		if len(rb.SourcePaths) > 0 {
			d.AddValueParam("$paths", table_types.StringValueFromString(strings.Join(rb.SourcePaths, ",")))
		}
	} else if operation.GetType() == types.OperationTypeDB {
		db, ok := operation.(*types.DeleteBackupOperation)
		if !ok {
			log.Fatalf("error cast operation to DeleteBackupOperation operation_id %s", operation.GetID())
		}

		d.AddValueParam(
			"$container_id", table_types.StringValueFromString(db.ContainerID),
		)

		d.AddValueParam(
			"$database",
			table_types.StringValueFromString(db.YdbConnectionParams.DatabaseName),
		)

		d.AddValueParam(
			"$endpoint",
			table_types.StringValueFromString(db.YdbConnectionParams.Endpoint),
		)

		d.AddValueParam(
			"$backup_id",
			table_types.StringValueFromString(db.BackupID),
		)

		d.AddValueParam("$paths", table_types.StringValueFromString(db.PathPrefix))

	} else {
		log.Fatalf(
			"unknown operation type write to db operation_id %s, operation_type %s",
			operation.GetID(),
			operation.GetType().String(),
		)
	}

	return d
}

func BuildUpdateOperationQuery(operation types.Operation, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Operations",
	}
	d.AddUpdateId(table_types.StringValueFromString(operation.GetID()))
	d.AddValueParam(
		"$status", table_types.StringValueFromString(operation.GetState().String()),
	)
	d.AddValueParam(
		"$message",
		table_types.StringValueFromString(operation.GetMessage()),
	)
	if operation.GetAudit() != nil && operation.GetAudit().CompletedAt != nil {
		d.AddValueParam(
			"$completed_at",
			table_types.TimestampValueFromTime(operation.GetAudit().GetCompletedAt().AsTime()),
		)
	}
	if operation.GetUpdatedAt() != nil {
		d.AddValueParam(
			"$updated_at",
			table_types.TimestampValueFromTime(operation.GetUpdatedAt().AsTime()),
		)
	}
	return d
}

func BuildUpdateBackupQuery(backup types.Backup, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Backups",
	}
	d.AddUpdateId(table_types.StringValueFromString(backup.ID))
	d.AddValueParam("$status", table_types.StringValueFromString(backup.Status))
	d.AddValueParam("$message", table_types.StringValueFromString(backup.Message))
	if backup.Size != 0 {
		d.AddValueParam("$size", table_types.Int64Value(backup.Size))

	}
	if backup.AuditInfo != nil && backup.AuditInfo.CompletedAt != nil {
		d.AddValueParam("$completed_at", table_types.TimestampValueFromTime(backup.AuditInfo.CompletedAt.AsTime()))
	}
	return d
}

func BuildCreateBackupQuery(b types.Backup, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Backups",
	}
	d.AddValueParam("$id", table_types.StringValueFromString(b.ID))
	d.AddValueParam("$container_id", table_types.StringValueFromString(b.ContainerID))
	d.AddValueParam("$database", table_types.StringValueFromString(b.DatabaseName))
	d.AddValueParam("$endpoint", table_types.StringValueFromString(b.DatabaseEndpoint))
	d.AddValueParam("$s3_endpoint", table_types.StringValueFromString(b.S3Endpoint))
	d.AddValueParam("$s3_region", table_types.StringValueFromString(b.S3Region))
	d.AddValueParam("$s3_bucket", table_types.StringValueFromString(b.S3Bucket))
	d.AddValueParam("$s3_path_prefix", table_types.StringValueFromString(b.S3PathPrefix))
	d.AddValueParam("$status", table_types.StringValueFromString(b.Status))
	d.AddValueParam("$message", table_types.StringValueFromString(b.Message))
	d.AddValueParam("$size", table_types.Int64Value(b.Size))
	d.AddValueParam("$paths", table_types.StringValueFromString(strings.Join(b.SourcePaths, ",")))
	if b.ScheduleID != nil {
		d.AddValueParam("$schedule_id", table_types.StringValueFromString(*b.ScheduleID))
	}
	if b.AuditInfo != nil {
		d.AddValueParam("$initiated", table_types.StringValueFromString(b.AuditInfo.Creator))
		if b.AuditInfo.CreatedAt != nil {
			d.AddValueParam("$created_at", table_types.TimestampValueFromTime(b.AuditInfo.CreatedAt.AsTime()))
		}
		if b.AuditInfo.CompletedAt != nil {
			d.AddValueParam("$completed_at", table_types.TimestampValueFromTime(b.AuditInfo.CompletedAt.AsTime()))
		}
	}

	if b.ExpireAt != nil {
		d.AddValueParam("$expire_at", table_types.TimestampValueFromTime(*b.ExpireAt))
	}
	return d
}

func BuildCreateBackupScheduleQuery(schedule types.BackupSchedule, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "BackupSchedules",
	}
	d.AddValueParam("$id", table_types.StringValueFromString(schedule.ID))
	d.AddValueParam("$container_id", table_types.StringValueFromString(schedule.ContainerID))
	d.AddValueParam(
		"$database",
		table_types.StringValueFromString(schedule.DatabaseName),
	)
	d.AddValueParam(
		"$endpoint",
		table_types.StringValueFromString(schedule.DatabaseEndpoint),
	)
	d.AddValueParam("$status", table_types.StringValueFromString(schedule.Status))
	d.AddValueParam("$crontab", table_types.StringValueFromString(schedule.ScheduleSettings.SchedulePattern.Crontab))

	if schedule.Name != nil {
		d.AddValueParam("$name", table_types.StringValueFromString(*schedule.Name))
	}
	if schedule.ScheduleSettings.Ttl != nil {
		d.AddValueParam("$ttl", table_types.IntervalValueFromDuration(schedule.ScheduleSettings.Ttl.AsDuration()))
	}
	if len(schedule.SourcePaths) > 0 {
		d.AddValueParam("$paths", table_types.StringValueFromString(strings.Join(schedule.SourcePaths, ",")))
	}
	if len(schedule.SourcePathsToExclude) > 0 {
		d.AddValueParam(
			"$paths_to_exclude",
			table_types.StringValueFromString(strings.Join(schedule.SourcePathsToExclude, ",")),
		)
	}
	if schedule.Audit != nil {
		d.AddValueParam("$initiated", table_types.StringValueFromString(schedule.Audit.Creator))
		if schedule.Audit.CreatedAt != nil {
			d.AddValueParam("$created_at", table_types.TimestampValueFromTime(schedule.Audit.CreatedAt.AsTime()))
		}
		if schedule.Audit.CompletedAt != nil {
			d.AddValueParam("$completed_at", table_types.TimestampValueFromTime(schedule.Audit.CompletedAt.AsTime()))
		}
	}
	if schedule.ScheduleSettings.RecoveryPointObjective != nil {
		d.AddValueParam(
			"$recovery_point_objective",
			table_types.IntervalValueFromDuration(schedule.ScheduleSettings.RecoveryPointObjective.AsDuration()),
		)
	}
	if schedule.NextLaunch != nil {
		d.AddValueParam("$next_launch", table_types.TimestampValueFromTime(*schedule.NextLaunch))
	}
	return d
}

func BuildUpdateBackupScheduleQuery(schedule types.BackupSchedule, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "BackupSchedules",
	}
	d.AddUpdateId(table_types.StringValueFromString(schedule.ID))
	d.AddValueParam("$status", table_types.StringValueFromString(schedule.Status))
	d.AddValueParam("$crontab", table_types.StringValueFromString(schedule.ScheduleSettings.SchedulePattern.Crontab))

	if len(schedule.SourcePaths) > 0 {
		d.AddValueParam("$paths", table_types.StringValueFromString(strings.Join(schedule.SourcePaths, ",")))
	} else {
		d.AddValueParam("$paths", table_types.NullableStringValueFromString(nil))
	}
	if len(schedule.SourcePathsToExclude) > 0 {
		d.AddValueParam(
			"$paths_to_exclude",
			table_types.StringValueFromString(strings.Join(schedule.SourcePathsToExclude, ",")),
		)
	} else {
		d.AddValueParam("$paths_to_exclude", table_types.NullableStringValueFromString(nil))
	}
	if schedule.Name != nil {
		d.AddValueParam("$name", table_types.StringValueFromString(*schedule.Name))
	}

	if schedule.ScheduleSettings.Ttl != nil {
		d.AddValueParam("$ttl", table_types.IntervalValueFromDuration(schedule.ScheduleSettings.Ttl.AsDuration()))
	}
	if schedule.ScheduleSettings.RecoveryPointObjective != nil {
		d.AddValueParam(
			"$recovery_point_objective",
			table_types.IntervalValueFromDuration(schedule.ScheduleSettings.RecoveryPointObjective.AsDuration()),
		)
	}

	if schedule.NextLaunch != nil {
		d.AddValueParam("$next_launch", table_types.TimestampValueFromTime(*schedule.NextLaunch))
	}
	return d
}

func NewWriteTableQuery() WriteTableQuery {
	return &WriteTableQueryImpl{}
}

func (d *WriteTableQueryImpl) WithCreateBackup(backup types.Backup) WriteTableQuery {
	index := len(d.tableQueries)
	d.tableQueries = append(d.tableQueries, BuildCreateBackupQuery(backup, index))
	return d
}

func (d *WriteTableQueryImpl) WithUpdateBackup(backup types.Backup) WriteTableQuery {
	index := len(d.tableQueries)
	d.tableQueries = append(d.tableQueries, BuildUpdateBackupQuery(backup, index))
	return d
}

func (d *WriteTableQueryImpl) WithUpdateOperation(operation types.Operation) WriteTableQuery {
	index := len(d.tableQueries)
	d.tableQueries = append(d.tableQueries, BuildUpdateOperationQuery(operation, index))
	return d
}

func (d *WriteTableQueryImpl) WithCreateOperation(operation types.Operation) WriteTableQuery {
	index := len(d.tableQueries)
	d.tableQueries = append(d.tableQueries, BuildCreateOperationQuery(operation, index))
	return d
}

func (d *WriteTableQueryImpl) WithUpdateBackupSchedule(schedule types.BackupSchedule) WriteTableQuery {
	index := len(d.tableQueries)
	d.tableQueries = append(d.tableQueries, BuildUpdateBackupScheduleQuery(schedule, index))
	return d
}

func (d *WriteTableQueryImpl) WithCreateBackupSchedule(schedule types.BackupSchedule) WriteTableQuery {
	index := len(d.tableQueries)
	d.tableQueries = append(d.tableQueries, BuildCreateBackupScheduleQuery(schedule, index))
	return d
}

func ProcessUpsertQuery(
	queryStrings *[]string, allParams *[]table.ParameterOption, t *WriteSingleTableQueryImpl,
) error {
	if len(t.upsertFields) == 0 {
		return errors.New("no fields to upsert")
	}
	if len(t.tableName) == 0 {
		return errors.New("no table")
	}
	*queryStrings = append(
		*queryStrings, fmt.Sprintf(
			"UPSERT INTO %s (%s) VALUES (%s)", t.tableName, strings.Join(t.upsertFields, ", "),
			strings.Join(t.GetParamNames(), ", "),
		),
	)
	*allParams = append(*allParams, t.tableQueryParams...)
	return nil
}

func ProcessUpdateQuery(
	queryStrings *[]string, allParams *[]table.ParameterOption, t *WriteSingleTableQueryImpl,
) error {
	if len(t.upsertFields) == 0 {
		return errors.New("no fields to upsert")
	}
	if len(t.tableName) == 0 {
		return errors.New("no table")
	}
	paramNames := t.GetParamNames()
	keyParam := fmt.Sprintf("id = %s", (*t.updateParam).Name())
	updates := make([]string, 0)
	for i := range t.upsertFields {
		updates = append(updates, fmt.Sprintf("%s = %s", t.upsertFields[i], paramNames[i]))
	}
	*queryStrings = append(
		*queryStrings, fmt.Sprintf(
			"UPDATE %s SET %s WHERE %s", t.tableName, strings.Join(updates, ", "), keyParam,
		),
	)
	*allParams = append(*allParams, *t.updateParam)
	*allParams = append(*allParams, t.tableQueryParams...)
	return nil
}

func (d *WriteTableQueryImpl) FormatQuery(ctx context.Context) (*FormatQueryResult, error) {
	queryStrings := make([]string, 0)
	allParams := make([]table.ParameterOption, 0)
	for _, t := range d.tableQueries {
		var err error
		if t.updateParam == nil {
			err = ProcessUpsertQuery(&queryStrings, &allParams, &t)
		} else {
			err = ProcessUpdateQuery(&queryStrings, &allParams, &t)
		}
		if err != nil {
			return nil, err
		}
	}
	res := strings.Join(queryStrings, ";\n")
	xlog.Debug(ctx, "write query", zap.String("yql", res))
	return &FormatQueryResult{
		QueryText:   res,
		QueryParams: table.NewQueryParameters(allParams...),
	}, nil
}
