package queries

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
	"strings"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"ydbcp/internal/util/xlog"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

const (
	DEFAULT_PAGE_SIZE = 50
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

func NewOrderSpec(order *pb.ListBackupsOrder) (*OrderSpec, error) {
	if order == nil {
		return &OrderSpec{
			Field: "created_at",
			Desc:  true,
		}, nil
	}
	var spec OrderSpec
	switch order.GetField() {
	case pb.BackupField_DATABASE_NAME:
		spec.Field = "database"
	case pb.BackupField_STATUS:
		spec.Field = "status"
	case pb.BackupField_CREATED_AT:
		spec.Field = "created_at"
	case pb.BackupField_EXPIRE_AT:
		spec.Field = "expire_at"
	case pb.BackupField_COMPLETED_AT:
		spec.Field = "completed_at"
	default:
		return nil, status.Error(
			codes.Internal, fmt.Sprintf("internal error: did not expect pb.BackupField_%s", order.GetField().String()),
		)
	}
	spec.Desc = order.GetDesc()
	return &spec, nil
}

type ReadTableQueryImpl struct {
	rawQuery         *string
	tableName        string
	filters          [][]table_types.Value
	filterFields     []string
	isLikeFilter     map[string]bool
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
	d.isLikeFilter = make(map[string]bool)
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
				d.isLikeFilter[filter.Field] = true
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
			} else if d.isLikeFilter[d.filterFields[i]] {
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

	xlog.Debug(ctx, "read query", zap.String("yql", res))
	return &FormatQueryResult{
		QueryText:   res,
		QueryParams: table.NewQueryParameters(d.tableQueryParams...),
	}, nil
}
