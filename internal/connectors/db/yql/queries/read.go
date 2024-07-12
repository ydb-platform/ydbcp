package queries

import (
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"strings"
)

var (
	AllBackupFields = []string{
		"id", "container_id", "database",
		"initiated", "created_at", "compileted_at",
		"s3_bucket", "s3_key_prefix", "status",
		"paths", "operation_id",
	}
)

type StringLike interface {
	~string
}

type QueryFilter[T StringLike] struct {
	Field  string
	Values []T
	IsLike bool
}

type FormatQueryResult struct {
	QueryText   string
	QueryParams *table.QueryParameters
}

type ReadTableQuery interface {
	MakeFilterString() string
	FormatQuery() (*FormatQueryResult, error)
}

type ReadTableQueryImpl struct {
	tableName        string
	selectFields     []string
	filters          [][]string
	filterFields     []string
	isLikeFilter     map[string]bool
	tableQueryParams []table.ParameterOption
}

type ReadTableQueryOption func(*ReadTableQueryImpl)

func MakeReadTableQuery(options ...ReadTableQueryOption) *ReadTableQueryImpl {
	d := &ReadTableQueryImpl{}
	d.filters = make([][]string, 0)
	d.filterFields = make([]string, 0)
	d.isLikeFilter = make(map[string]bool)
	for _, opt := range options {
		opt(d)
	}
	return d
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

func WithQueryFilters[T StringLike](filters ...QueryFilter[T]) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		for _, filter := range filters {
			d.filterFields = append(d.filterFields, filter.Field)
			newFilters := make([]string, 0, len(filter.Values))
			for _, value := range filter.Values {
				newFilters = append(newFilters, string(value))
			}
			d.filters = append(d.filters, newFilters)
			if filter.IsLike {
				d.isLikeFilter[filter.Field] = true
			}
		}
	}
}

func (d *ReadTableQueryImpl) AddTableQueryParam(paramValue string) string {
	paramName := fmt.Sprintf("$param%d", len(d.tableQueryParams))
	d.tableQueryParams = append(
		d.tableQueryParams, table.ValueParam(paramName, table_types.StringValueFromString(paramValue)),
	)
	return paramName
}

func (d *ReadTableQueryImpl) MakeFilterString() string {
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
	return strings.Join(filterStrings, " AND ")
}

func (d *ReadTableQueryImpl) FormatQuery() (*FormatQueryResult, error) {
	if len(d.selectFields) == 0 {
		return nil, errors.New("No fields to select")
	}
	if len(d.tableName) == 0 {
		return nil, errors.New("No table")
	}
	if len(d.filters) == 0 {
		return nil, errors.New("No filters")
	}
	res := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		strings.Join(d.selectFields, ", "),
		d.tableName,
		d.MakeFilterString(),
	)
	return &FormatQueryResult{
		QueryText:   res,
		QueryParams: table.NewQueryParameters(d.tableQueryParams...),
	}, nil
}
