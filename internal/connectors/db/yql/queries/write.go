package queries

import (
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"strings"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
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
}

func (d *WriteSingleTableQueryImpl) AddValueParam(name string, value table_types.Value) {
	d.upsertFields = append(d.upsertFields, name[1:])
	d.tableQueryParams = append(d.tableQueryParams, table.ValueParam(fmt.Sprintf("%s_%d", name, d.index), value))
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
	d.AddValueParam("$id", table_types.UUIDValue(operation.GetId()))
	d.AddValueParam("$type", table_types.StringValueFromString(string(operation.GetType())))
	d.AddValueParam(
		"$status", table_types.StringValueFromString(operation.GetState().String()),
	)
	if operation.GetType() == types.OperationType("TB") {
		tb := operation.(*types.TakeBackupOperation)
		d.AddValueParam(
			"$container_id", table_types.StringValueFromString(tb.ContainerID),
		)
		d.AddValueParam(
			"$database",
			table_types.StringValueFromString(tb.YdbConnectionParams.DatabaseName),
		)
		d.AddValueParam(
			"$backup_id",
			table_types.UUIDValue(tb.BackupId),
		)
		d.AddValueParam(
			"$initiated",
			table_types.StringValueFromString(""), //TODO
		)
		d.AddValueParam(
			"$created_at",
			table_types.StringValueFromString(""), //TODO
		)
		d.AddValueParam(
			"$operation_id",
			table_types.StringValueFromString(tb.YdbOperationId),
		)
	}

	return d
}

func BuildUpdateOperationQuery(operation types.Operation, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Operations",
	}
	d.AddValueParam("$id", table_types.UUIDValue(operation.GetId()))
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
	d.AddValueParam("$id", table_types.UUIDValue(backup.ID))
	d.AddValueParam("$status", table_types.StringValueFromString(backup.Status))
	return d
}

func BuildCreateBackupQuery(b types.Backup, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Backups",
	}
	d.AddValueParam("$id", table_types.UUIDValue(b.ID))
	d.AddValueParam("$container_id", table_types.StringValueFromString(b.ContainerID))
	d.AddValueParam("$database", table_types.StringValueFromString(b.DatabaseName))
	d.AddValueParam("$initiated", table_types.StringValueFromString("")) // TODO
	d.AddValueParam("$s3_endpoint", table_types.StringValueFromString(b.S3Endpoint))
	d.AddValueParam("$s3_region", table_types.StringValueFromString(b.S3Region))
	d.AddValueParam("$s3_bucket", table_types.StringValueFromString(b.S3Bucket))
	d.AddValueParam("$s3_path_prefix", table_types.StringValueFromString(b.S3PathPrefix))
	d.AddValueParam("$status", table_types.StringValueFromString(b.Status))
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

func (d *WriteSingleTableQueryImpl) DeclareParameters() string {
	declares := make([]string, len(d.tableQueryParams))
	for i, param := range d.tableQueryParams {
		declares[i] = fmt.Sprintf("DECLARE %s AS %s", param.Name(), param.Value().Type().String())
	}
	return strings.Join(declares, ";\n")
}

func (d *WriteTableQueryImpl) FormatQuery(ctx context.Context) (*FormatQueryResult, error) {
	queryStrings := make([]string, len(d.tableQueries))
	allParams := make([]table.ParameterOption, 0)
	for i, t := range d.tableQueries {
		if len(t.upsertFields) == 0 {
			return nil, errors.New("No fields to upsert")
		}
		if len(t.tableName) == 0 {
			return nil, errors.New("No table")
		}
		declares := t.DeclareParameters()
		paramNames := t.GetParamNames()
		keyParam := fmt.Sprintf("%s = %s", t.upsertFields[0], paramNames[0])
		updates := make([]string, 0)
		for j := 1; j < len(t.upsertFields); j++ {
			updates = append(updates, fmt.Sprintf("%s = %s", t.upsertFields[j], paramNames[j]))
		}
		queryStrings[i] = fmt.Sprintf(
			"%s;\nUPDATE %s SET %s WHERE %s", declares, t.tableName, strings.Join(updates, ", "), keyParam,
		)
		for _, p := range t.tableQueryParams {
			allParams = append(allParams, p)
		}
	}
	res := strings.Join(queryStrings, ";\n")
	xlog.Debug(ctx, "write query", zap.String("yql", res))
	return &FormatQueryResult{
		QueryText:   res,
		QueryParams: table.NewQueryParameters(allParams...),
	}, nil
}
