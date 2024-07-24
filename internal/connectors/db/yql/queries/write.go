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
			table_types.TimestampValueFromTime(tb.CreatedAt),
		)
		d.AddValueParam(
			"$operation_id",
			table_types.StringValueFromString(tb.YdbOperationId),
		)
	} else {
		panic("Implement me")
	}

	return d
}

func BuildUpdateOperationQuery(operation types.Operation, index int) WriteSingleTableQueryImpl {
	d := WriteSingleTableQueryImpl{
		index:     index,
		tableName: "Operations",
	}
	d.AddUpdateId(table_types.UUIDValue(operation.GetId()))
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
	d.AddUpdateId(table_types.UUIDValue(backup.ID))
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
	declares := make([]string, 0)
	if d.updateParam != nil {
		declares = append(
			declares,
			fmt.Sprintf("DECLARE %s AS %s", (*d.updateParam).Name(), (*d.updateParam).Value().Type().String()),
		)
	}
	for _, param := range d.tableQueryParams {
		declares = append(
			declares, fmt.Sprintf("DECLARE %s AS %s", param.Name(), param.Value().Type().String()),
		)
	}
	return strings.Join(declares, ";\n")
}

func ProcessUpsertQuery(
	queryStrings *[]string, allParams *[]table.ParameterOption, t *WriteSingleTableQueryImpl,
) error {
	if len(t.upsertFields) == 0 {
		return errors.New("No fields to upsert")
	}
	if len(t.tableName) == 0 {
		return errors.New("No table")
	}
	declares := t.DeclareParameters()
	*queryStrings = append(
		*queryStrings, fmt.Sprintf(
			"%s;\nUPSERT INTO %s (%s) VALUES (%s)", declares, t.tableName, strings.Join(t.upsertFields, ", "),
			strings.Join(t.GetParamNames(), ", "),
		),
	)
	for _, p := range t.tableQueryParams {
		*allParams = append(*allParams, p)
	}
	return nil
}

func ProcessUpdateQuery(
	queryStrings *[]string, allParams *[]table.ParameterOption, t *WriteSingleTableQueryImpl,
) error {
	if len(t.upsertFields) == 0 {
		return errors.New("No fields to upsert")
	}
	if len(t.tableName) == 0 {
		return errors.New("No table")
	}
	declares := t.DeclareParameters()
	paramNames := t.GetParamNames()
	keyParam := fmt.Sprintf("id = %s", (*t.updateParam).Name())
	updates := make([]string, 0)
	for i := range t.upsertFields {
		updates = append(updates, fmt.Sprintf("%s = %s", t.upsertFields[i], paramNames[i]))
	}
	*queryStrings = append(
		*queryStrings, fmt.Sprintf(
			"%s;\nUPDATE %s SET %s WHERE %s", declares, t.tableName, strings.Join(updates, ", "), keyParam,
		),
	)
	*allParams = append(*allParams, *t.updateParam)
	for _, p := range t.tableQueryParams {
		*allParams = append(*allParams, p)
	}
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
