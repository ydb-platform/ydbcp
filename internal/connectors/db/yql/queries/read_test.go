package queries

import (
	"context"
	"testing"
	ydbcp "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestQueryBuilderRead(t *testing.T) {
	const (
		queryString = `SELECT * FROM table1 WHERE (column1 = $param0 OR column1 = $param1) AND (column2 = $param2 OR column2 = $param3)`
	)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$param0", table_types.StringValueFromString("value1")),
			table.ValueParam("$param1", table_types.StringValueFromString("value2")),
			table.ValueParam("$param2", table_types.StringValueFromString("xxx")),
			table.ValueParam("$param3", table_types.StringValueFromString("yyy")),
		)
	)
	builder := NewReadTableQuery(
		WithTableName("table1"),
		WithQueryFilters(
			QueryFilter{
				Field: "column1",
				Values: []table_types.Value{
					table_types.StringValueFromString("value1"),
					table_types.StringValueFromString("value2"),
				},
			},
		),
		WithQueryFilters(
			QueryFilter{
				Field: "column2",
				Values: []table_types.Value{
					table_types.StringValueFromString("xxx"),
					table_types.StringValueFromString("yyy"),
				},
			},
		),
	)
	query, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, queryString, query.QueryText,
		"bad query format",
	)
	assert.Equal(t, queryParams, query.QueryParams, "bad query params")
}

func TestQueryBuilderLike(t *testing.T) {
	const (
		queryString = `SELECT * FROM table1 WHERE (database LIKE "%" || $param0 || "%") AND (column2 = $param1)`
	)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$param0", table_types.StringValueFromString("value1")),
			table.ValueParam("$param1", table_types.StringValueFromString("value2")),
		)
	)
	builder := NewReadTableQuery(
		WithTableName("table1"),
		WithQueryFilters(
			QueryFilter{
				Field:  "database",
				IsLike: true,
				Values: []table_types.Value{
					table_types.StringValueFromString("value1"),
				},
			},
		),
		WithQueryFilters(
			QueryFilter{
				Field: "column2",
				Values: []table_types.Value{
					table_types.StringValueFromString("value2"),
				},
			},
		),
	)
	query, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, queryString, query.QueryText,
		"bad query format",
	)
	assert.Equal(t, queryParams, query.QueryParams, "bad query params")
}

func TestQueryBuilderOrderBy(t *testing.T) {
	const (
		q1 = `SELECT * FROM table1 ORDER BY field`
		q2 = `SELECT * FROM table1 ORDER BY field DESC`
	)
	builder := NewReadTableQuery(
		WithTableName("table1"),
		WithOrderBy(
			OrderSpec{
				Field: "field",
				Desc:  false,
			},
		),
	)
	query, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, q1, query.QueryText,
		"bad query format",
	)
	builder = NewReadTableQuery(
		WithTableName("table1"),
		WithOrderBy(
			OrderSpec{
				Field: "field",
				Desc:  true,
			},
		),
	)
	query, err = builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, q2, query.QueryText,
		"bad query format",
	)
}

func TestQueryBuilderRawQueryModifiers(t *testing.T) {
	const (
		query     = `SELECT * FROM table1`
		fullQuery = `SELECT * FROM table1 WHERE (col1 = $param0) AND (col2 = $param1) ORDER BY col3 DESC`
	)
	builder := NewReadTableQuery(
		WithTableName("table1"),
		WithRawQuery(query),
		WithQueryFilters(
			QueryFilter{
				Field: "col1",
				Values: []table_types.Value{
					table_types.StringValueFromString("value1"),
				},
			},
			QueryFilter{
				Field: "col2",
				Values: []table_types.Value{
					table_types.StringValueFromString("value1"),
				},
			},
		),
		WithOrderBy(
			OrderSpec{
				Field: "col3",
				Desc:  true,
			},
		),
	)
	fq, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, fullQuery, fq.QueryText,
		"bad query format",
	)
}

func TestQueryBuilderPagination(t *testing.T) {
	const (
		query    = `SELECT * FROM table1 WHERE (col1 = $param0) ORDER BY col3 LIMIT 5 OFFSET 10`
		noOffset = `SELECT * FROM table1 LIMIT 5`
	)
	builder := NewReadTableQuery(
		WithTableName("table1"),
		WithQueryFilters(
			QueryFilter{
				Field: "col1",
				Values: []table_types.Value{
					table_types.StringValueFromString("value1"),
				},
			},
		),
		WithOrderBy(
			OrderSpec{
				Field: "col3",
			},
		),
		WithPageSpec(
			PageSpec{
				Limit:  5,
				Offset: 10,
			},
		),
	)
	fq, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, query, fq.QueryText,
		"bad query format",
	)
	builder = NewReadTableQuery(
		WithTableName("table1"),
		WithPageSpec(
			PageSpec{
				Limit: 5,
			},
		),
	)
	fq, err = builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, noOffset, fq.QueryText,
		"bad query format",
	)
}

func TestOrderSpec(t *testing.T) {
	const (
		query = `SELECT * FROM table1 ORDER BY created_at DESC`
	)
	pbOrder := &ydbcp.ListBackupsOrder{
		Field: ydbcp.BackupField_CREATED_AT,
		Desc:  true,
	}
	spec, err := NewOrderSpec(pbOrder)
	assert.Empty(t, err)
	builder := NewReadTableQuery(
		WithTableName("table1"),
		WithOrderBy(*spec),
	)
	fq, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, query, fq.QueryText,
		"bad query format",
	)
}

func TestQueryBuilderOperator(t *testing.T) {
	const (
		queryString = `SELECT * FROM table1 WHERE (created_at >= $param0) AND (created_at <= $param1)`
	)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$param0", table_types.StringValueFromString("2021-01-01T00:00:00Z")),
			table.ValueParam("$param1", table_types.StringValueFromString("2021-12-31T23:59:59Z")),
		)
	)
	builder := NewReadTableQuery(
		WithTableName("table1"),
		WithQueryFilters(
			QueryFilter{
				Field:    "created_at",
				Operator: ">=",
				Values: []table_types.Value{
					table_types.StringValueFromString("2021-01-01T00:00:00Z"),
				},
			},
			QueryFilter{
				Field:    "created_at",
				Operator: "<=",
				Values: []table_types.Value{
					table_types.StringValueFromString("2021-12-31T23:59:59Z"),
				},
			},
		),
	)
	query, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, queryString, query.QueryText,
		"bad query format",
	)
	assert.Equal(t, queryParams, query.QueryParams, "bad query params")
}

func TestIndex(t *testing.T) {
	const (
		query = `SELECT * FROM table1 VIEW index`
	)
	builder := NewReadTableQuery(
		WithTableName("table1"),
		WithIndex("index"),
	)
	fq, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, query, fq.QueryText,
		"bad query format",
	)
}
