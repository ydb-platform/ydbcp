package queries

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"testing"
	"ydbcp/internal/types"
)

func TestQueryBuilder_Write(t *testing.T) {
	const (
		queryString = `DECLARE $id_0 AS Uuid;
DECLARE $status_0 AS String;
UPDATE Backups SET status = $status_0 WHERE id = $id_0;
DECLARE $id_1 AS Uuid;
DECLARE $status_1 AS String;
DECLARE $message_1 AS String;
UPDATE Operations SET status = $status_1, message = $message_1 WHERE id = $id_1`
	)
	opId := types.GenerateObjectID()
	backupId := types.GenerateObjectID()
	op := types.TakeBackupOperation{
		Id:      opId,
		State:   "Done",
		Message: "Abcde",
	}
	backup := types.Backup{
		ID:     backupId,
		Status: "Available",
	}
	builder := NewWriteTableQuery().
		WithUpdateBackup(backup).
		WithUpdateOperation(&op)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$id_0", table_types.UUIDValue(backupId)),
			table.ValueParam("$status_0", table_types.StringValueFromString("Available")),
			table.ValueParam("$id_1", table_types.UUIDValue(opId)),
			table.ValueParam("$status_1", table_types.StringValueFromString("Done")),
			table.ValueParam("$message_1", table_types.StringValueFromString("Abcde")),
		)
	)
	query, err := builder.FormatQuery(context.Background())
	assert.Empty(t, err)
	assert.Equal(
		t, queryString, query.QueryText,
		"bad query format",
	)
	assert.Equal(t, queryParams, query.QueryParams, "bad query params")
}
