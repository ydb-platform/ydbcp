package queries

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestQueryBuilder_Read(t *testing.T) {
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

func TestQueryBuilder_Order(t *testing.T) {
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

func TestQueryBuilder_RawQueryModifiers(t *testing.T) {
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
