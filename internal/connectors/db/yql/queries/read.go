package queries

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

// TODO: do not do this and always select all fields
var (
	AllBackupFields = []string{
		"id", "container_id", "database", "endpoint",
		"initiated", "created_at", "completed_at",
		"s3_endpoint", "s3_region", "s3_bucket",
		"s3_path_prefix", "status", "paths", "message",
		"size", "schedule_id", "expire_at",
	}
	AllOperationFields = []string{
		"id", "type", "container_id", "database", "endpoint", "backup_id",
		"initiated", "created_at", "completed_at", "status", "message",
		"paths", "operation_id", "paths_to_exclude", "updated_at",
	}
	AllBackupScheduleFields = []string{
		"id", "container_id", "database", "endpoint", "name", "active", "crontab", "ttl", "paths", "paths_to_exclude",
		"initiated", "created_at", "recovery_point_objective", "next_launch",
	}
)

type QueryFilter struct {
	Field  string
	Values []table_types.Value
	IsLike bool
}

type FormatQueryResult struct {
	QueryText   string
	QueryParams *table.QueryParameters
}

type ReadTableQuery interface {
	MakeFilterString() string
	FormatQuery(ctx context.Context) (*FormatQueryResult, error)
}

type ReadTableQueryImpl struct {
	rawQuery         *string
	tableName        string
	selectFields     []string
	filters          [][]table_types.Value
	filterFields     []string
	isLikeFilter     map[string]bool
	tableQueryParams []table.ParameterOption
}

type ReadTableQueryOption func(*ReadTableQueryImpl)

func NewReadTableQuery(options ...ReadTableQueryOption) *ReadTableQueryImpl {
	d := &ReadTableQueryImpl{}
	d.filters = make([][]table_types.Value, 0)
	d.filterFields = make([]string, 0)
	d.isLikeFilter = make(map[string]bool)
	for _, opt := range options {
		opt(d)
	}
	return d
}

func WithRawQuery(rawQuery string) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		d.rawQuery = &rawQuery
	}
}

func WithTableName(tableName string) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		d.tableName = tableName
	}
}

func WithSelectFields(fields ...string) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		d.selectFields = fields
	}
}

func WithQueryFilters(filters ...QueryFilter) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		for _, filter := range filters {
			d.filterFields = append(d.filterFields, filter.Field)
			newFilters := make([]table_types.Value, 0, len(filter.Values))
			newFilters = append(newFilters, filter.Values...)
			d.filters = append(d.filters, newFilters)
			if filter.IsLike {
				d.isLikeFilter[filter.Field] = true
			}
		}
	}
}

func (d *ReadTableQueryImpl) AddTableQueryParam(paramValue table_types.Value) string {
	paramName := fmt.Sprintf("$param%d", len(d.tableQueryParams))
	d.tableQueryParams = append(
		d.tableQueryParams, table.ValueParam(paramName, paramValue),
	)
	return paramName
}

func (d *ReadTableQueryImpl) MakeFilterString() string {
	if len(d.filters) == 0 {
		return ""
	}
	filterStrings := make([]string, 0, len(d.filters))
	for i := 0; i < len(d.filterFields); i++ {
		fieldFilterStrings := make([]string, 0, len(d.filters[i]))
		op := "="
		if d.isLikeFilter[d.filterFields[i]] {
			op = "LIKE"
		}
		for _, value := range d.filters[i] {
			paramName := d.AddTableQueryParam(value)
			fieldFilterStrings = append(fieldFilterStrings, fmt.Sprintf("%s %s %s", d.filterFields[i], op, paramName))
		}
		filterStrings = append(filterStrings, fmt.Sprintf("(%s)", strings.Join(fieldFilterStrings, " OR ")))
	}
	return fmt.Sprintf(" WHERE %s", strings.Join(filterStrings, " AND "))
}

func (d *ReadTableQueryImpl) FormatQuery(ctx context.Context) (*FormatQueryResult, error) {
	var res string
	filter := d.MakeFilterString()
	if d.rawQuery == nil {
		if len(d.selectFields) == 0 {
			return nil, errors.New("no fields to select")
		}
		if len(d.tableName) == 0 {
			return nil, errors.New("no table")
		}
		res = fmt.Sprintf(
			"SELECT %s FROM %s%s",
			strings.Join(d.selectFields, ", "),
			d.tableName,
			filter,
		)
	} else {
		res = fmt.Sprintf("%s%s", *d.rawQuery, filter)
	}
	xlog.Debug(ctx, "read query", zap.String("yql", res))
	return &FormatQueryResult{
		QueryText:   res,
		QueryParams: table.NewQueryParameters(d.tableQueryParams...),
	}, nil
}
