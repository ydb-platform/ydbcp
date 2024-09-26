package queries

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
	"strings"

	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

const (
	DEFAULT_PAGE_SIZE = 50
)

type QueryFilter struct {
	Field  string
	Values []table_types.Value
	IsLike bool
}

type FormatQueryResult struct {
	QueryText           string
	QueryParams         *table.QueryParameters
	ExpectedUpdateStats map[string]uint64
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

func NewPageSpec(pageSize uint32, pageToken string) (*PageSpec, error) {
	var pageSpec PageSpec
	if pageSize == 0 {
		pageSpec.Limit = DEFAULT_PAGE_SIZE
	} else {
		pageSpec.Limit = uint64(pageSize)
	}
	if pageToken != "" {
		offset, err := strconv.ParseUint(pageToken, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "can't parse page token")
		}
		pageSpec.Offset = offset
	}
	return &pageSpec, nil
}

type ReadTableQueryImpl struct {
	rawQuery         *string
	tableName        string
	filters          [][]table_types.Value
	filterFields     []string
	isLikeFilter     map[string]bool
	orderBy          *OrderSpec
	pageSpec         *PageSpec
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

func (d *ReadTableQueryImpl) FormatQuery(ctx context.Context) (*FormatQueryResult, error) {
	var res string
	filter := d.MakeFilterString()

	if d.rawQuery == nil {
		if len(d.tableName) == 0 {
			return nil, errors.New("no table")
		}
		res = fmt.Sprintf(
			"SELECT * FROM %s%s",
			d.tableName,
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

	xlog.Debug(ctx, "read query", zap.String("yql", res))
	return &FormatQueryResult{
		QueryText:   res,
		QueryParams: table.NewQueryParameters(d.tableQueryParams...),
	}, nil
}
