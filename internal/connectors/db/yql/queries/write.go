package queries

import (
	"context"
	"errors"
	"fmt"
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
	WithUpdateBackup(backup types.Backup) WriteTableQuery
	WithUpdateOperation(operation types.Operation) WriteTableQuery
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
	tb, ok := operation.(*types.TakeBackupOperation)
	if !ok || operation.GetType() != types.OperationTypeTB {
		panic("Incorrect operation type")
	}
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Operations",
	}
	d.AddValueParam("$id", table_types.StringValueFromString(operation.GetID()))
	d.AddValueParam("$type", table_types.StringValueFromString(string(operation.GetType())))
	d.AddValueParam("$status", table_types.StringValueFromString(operation.GetState().String()))

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
		table_types.StringValueFromString(tb.BackupId),
	)
	d.AddValueParam(
		"$initiated",
		table_types.StringValueFromString(""), //TODO
	)
	d.AddValueParam(
		"$created_at",
		table_types.TimestampValueFromTime(tb.CreatedAt),
	)
	d.AddValueParam(
		"$operation_id",
		table_types.StringValueFromString(tb.YdbOperationId),
	)
	d.AddValueParam("$message", table_types.StringValueFromString(tb.Message))

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
	return d
}

func BuildUpdateBackupQuery(backup types.Backup, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Backups",
	}
	d.AddUpdateId(table_types.StringValueFromString(backup.ID))
	d.AddValueParam("$status", table_types.StringValueFromString(backup.Status))
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
	d.AddValueParam("$initiated", table_types.StringValueFromString("")) // TODO
	d.AddValueParam("$s3_endpoint", table_types.StringValueFromString(b.S3Endpoint))
	d.AddValueParam("$s3_region", table_types.StringValueFromString(b.S3Region))
	d.AddValueParam("$s3_bucket", table_types.StringValueFromString(b.S3Bucket))
	d.AddValueParam("$s3_path_prefix", table_types.StringValueFromString(b.S3PathPrefix))
	d.AddValueParam("$status", table_types.StringValueFromString(b.Status))
	d.AddValueParam("$message", table_types.StringValueFromString(b.Message))
	return d
}

type WriteTableQueryImplOption func(*WriteTableQueryImpl)

type WriteTableQueryMockOption func(*WriteTableQueryMock)

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
