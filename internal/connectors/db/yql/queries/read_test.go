package queries

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"testing"
)

func TestQueryBuilder_Read(t *testing.T) {
	const (
		queryString = `DECLARE $param0 AS String;
DECLARE $param1 AS String;
DECLARE $param2 AS String;
DECLARE $param3 AS String;
SELECT column1, column2, column3 FROM table1 WHERE (column1 = $param0 OR column1 = $param1) AND (column2 = $param2 OR column2 = $param3)`
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
		WithSelectFields("column1", "column2", "column3"),
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
