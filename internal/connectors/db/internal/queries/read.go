package queries

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

type QueryFilter struct {
	Field    string
	Values   []table_types.Value
	IsLike   bool
	Operator string // Optional: ">=", "<=", ">", "<". Defaults to "=" when empty.
}

type FormatQueryResult struct {
	QueryText   string
	QueryParams *table.QueryParameters
}

type ReadTableQuery interface {
	MakeFilterString() string
	FormatQuery(ctx context.Context) (*FormatQueryResult, error)
}

type OrderSpec struct {
	Field string
	Desc  bool
}

type PageSpec struct {
	Limit  uint64
	Offset uint64
}

type ReadTableQueryImpl struct {
	rawQuery         *string
	tableName        string
	filters          [][]table_types.Value
	filterFields     []string
	isLikeFilter     map[int]bool
	filterOperators  map[int]string
	index            *string
	orderBy          *OrderSpec
	pageSpec         *PageSpec
	tableQueryParams []table.ParameterOption
}

type ReadTableQueryOption func(*ReadTableQueryImpl)

func NewReadTableQuery(options ...ReadTableQueryOption) *ReadTableQueryImpl {
	d := &ReadTableQueryImpl{}
	d.filters = make([][]table_types.Value, 0)
	d.filterFields = make([]string, 0)
	d.isLikeFilter = make(map[int]bool)
	d.filterOperators = make(map[int]string)

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

func WithParameters(params ...table.ParameterOption) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		for _, param := range params {
			d.tableQueryParams = append(
				d.tableQueryParams, param,
			)
		}
	}
}

func WithTableName(tableName string) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		d.tableName = tableName
	}
}

func WithQueryFilters(filters ...QueryFilter) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		for _, filter := range filters {
			idx := len(d.filterFields)
			d.filterFields = append(d.filterFields, filter.Field)
			newFilters := make([]table_types.Value, 0, len(filter.Values))
			newFilters = append(newFilters, filter.Values...)
			d.filters = append(d.filters, newFilters)
			if filter.IsLike {
				d.isLikeFilter[idx] = true
			}
			if filter.Operator != "" {
				d.filterOperators[idx] = filter.Operator
			}
		}
	}
}

func WithOrderBy(spec OrderSpec) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		d.orderBy = &spec
	}
}

func WithPageSpec(spec PageSpec) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		d.pageSpec = &spec
	}
}

func WithIndex(index string) ReadTableQueryOption {
	return func(d *ReadTableQueryImpl) {
		d.index = &index
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
		for _, value := range d.filters[i] {
			paramName := d.AddTableQueryParam(value)
			op := "="
			if customOp, ok := d.filterOperators[i]; ok {
				op = customOp
			} else if d.isLikeFilter[i] {
				op = "LIKE"
				paramName = fmt.Sprintf("\"%%\" || %s || \"%%\"", paramName)
			}
			fieldFilterStrings = append(fieldFilterStrings, fmt.Sprintf("%s %s %s", d.filterFields[i], op, paramName))
		}
		filterStrings = append(filterStrings, fmt.Sprintf("(%s)", strings.Join(fieldFilterStrings, " OR ")))
	}
	return fmt.Sprintf(" WHERE %s", strings.Join(filterStrings, " AND "))
}

func (d *ReadTableQueryImpl) FormatOrder() *string {
	if d.orderBy == nil {
		return nil
	}
	descStr := ""
	if d.orderBy.Desc == true {
		descStr = " DESC"
	}
	orderBy := fmt.Sprintf(" ORDER BY %s%s", d.orderBy.Field, descStr)
	return &orderBy
}

func (d *ReadTableQueryImpl) FormatPage() *string {
	if d.pageSpec == nil {
		return nil
	}
	page := ""
	if d.pageSpec.Limit != 0 {
		page = fmt.Sprintf(" LIMIT %d", d.pageSpec.Limit)
	}
	if d.pageSpec.Offset != 0 {
		page += fmt.Sprintf(" OFFSET %d", d.pageSpec.Offset)
	}
	return &page
}

func (d *ReadTableQueryImpl) FormatTable() string {
	if d.index == nil {
		return d.tableName
	}
	return fmt.Sprintf("%s VIEW %s", d.tableName, *d.index)
}

func (d *ReadTableQueryImpl) FormatQuery(ctx context.Context) (*FormatQueryResult, error) {
	// Formatting must not accumulate filter parameters when reused.
	copy := *d
	copy.tableQueryParams = append([]table.ParameterOption(nil), d.tableQueryParams...)
	return copy.formatQuery(ctx)
}

func (d *ReadTableQueryImpl) formatQuery(ctx context.Context) (*FormatQueryResult, error) {
	var res string
	filter := d.MakeFilterString()
	if d.rawQuery == nil {
		if len(d.tableName) == 0 {
			return nil, errors.New("no table")
		}
		res = fmt.Sprintf(
			"SELECT * FROM %s%s",
			d.FormatTable(),
			filter,
		)
	} else {
		res = fmt.Sprintf("%s%s", *d.rawQuery, filter)
	}
	order := d.FormatOrder()
	if order != nil {
		res += *order
	}
	page := d.FormatPage()
	if page != nil {
		res += *page
	}

	xlog.Debug(ctx, "read query", zap.String(log_keys.YQL, res))
	return &FormatQueryResult{
		QueryText:   res,
		QueryParams: table.NewQueryParameters(d.tableQueryParams...),
	}, nil
}
