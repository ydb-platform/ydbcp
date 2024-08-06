package queries

import (
	"context"
	"testing"
	"time"
	"ydbcp/internal/types"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestQueryBuilder_UpdateUpdate(t *testing.T) {
	const (
		queryString = `UPDATE Backups SET status = $status_0 WHERE id = $id_0;
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

func TestQueryBuilder_CreateCreate(t *testing.T) {
	const (
		queryString = `UPSERT INTO Backups (id, container_id, database, endpoint, initiated, s3_endpoint, s3_region, s3_bucket, s3_path_prefix, status, message) VALUES ($id_0, $container_id_0, $database_0, $endpoint_0, $initiated_0, $s3_endpoint_0, $s3_region_0, $s3_bucket_0, $s3_path_prefix_0, $status_0, $message_0);
UPSERT INTO Operations (id, type, status, container_id, database, endpoint, backup_id, initiated, created_at, operation_id, message) VALUES ($id_1, $type_1, $status_1, $container_id_1, $database_1, $endpoint_1, $backup_id_1, $initiated_1, $created_at_1, $operation_id_1, $message_1)`
	)
	opId := types.GenerateObjectID()
	backupId := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		Id:          opId,
		ContainerID: "a",
		BackupId:    backupId,
		State:       "PENDING",
		Message:     "msg op",
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     "endpoint",
			DatabaseName: "dbname",
		},
		YdbOperationId:      "1234",
		SourcePaths:         nil,
		SourcePathToExclude: nil,
		CreatedAt:           time.Unix(0, 0),
	}
	backup := types.Backup{
		ID:               backupId,
		ContainerID:      "a",
		DatabaseName:     "b",
		DatabaseEndpoint: "g",
		S3Endpoint:       "c",
		S3Region:         "d",
		S3Bucket:         "e",
		S3PathPrefix:     "f",
		Status:           "Available",
		Message:          "msg backup",
	}
	builder := NewWriteTableQuery().
		WithCreateBackup(backup).
		WithCreateOperation(&tbOp)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$id_0", table_types.UUIDValue(backupId)),
			table.ValueParam("$container_id_0", table_types.StringValueFromString("a")),
			table.ValueParam("$database_0", table_types.StringValueFromString("b")),
			table.ValueParam("$endpoint_0", table_types.StringValueFromString("g")),
			table.ValueParam("$initiated_0", table_types.StringValueFromString("")),
			table.ValueParam("$s3_endpoint_0", table_types.StringValueFromString("c")),
			table.ValueParam("$s3_region_0", table_types.StringValueFromString("d")),
			table.ValueParam("$s3_bucket_0", table_types.StringValueFromString("e")),
			table.ValueParam("$s3_path_prefix_0", table_types.StringValueFromString("f")),
			table.ValueParam("$status_0", table_types.StringValueFromString("Available")),
			table.ValueParam("$message_0", table_types.StringValueFromString("msg backup")),
			table.ValueParam("$id_1", table_types.UUIDValue(opId)),
			table.ValueParam("$type_1", table_types.StringValueFromString("TB")),
			table.ValueParam(
				"$status_1", table_types.StringValueFromString(string(tbOp.State)),
			),
			table.ValueParam(
				"$container_id_1", table_types.StringValueFromString(tbOp.ContainerID),
			),
			table.ValueParam(
				"$database_1",
				table_types.StringValueFromString(tbOp.YdbConnectionParams.DatabaseName),
			),
			table.ValueParam(
				"$endpoint_1",
				table_types.StringValueFromString(tbOp.YdbConnectionParams.Endpoint),
			),
			table.ValueParam(
				"$backup_id_1",
				table_types.UUIDValue(tbOp.BackupId),
			),
			table.ValueParam(
				"$initiated_1",
				table_types.StringValueFromString(""),
			),
			table.ValueParam(
				"$created_at_1",
				table_types.TimestampValueFromTime(tbOp.CreatedAt),
			),
			table.ValueParam(
				"$operation_id_1",
				table_types.StringValueFromString(tbOp.YdbOperationId),
			),
			table.ValueParam("$message_1", table_types.StringValueFromString("msg op")),
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

func TestQueryBuilder_UpdateCreate(t *testing.T) {
	const (
		queryString = `UPDATE Backups SET status = $status_0 WHERE id = $id_0;
UPSERT INTO Operations (id, type, status, container_id, database, endpoint, backup_id, initiated, created_at, operation_id, message) VALUES ($id_1, $type_1, $status_1, $container_id_1, $database_1, $endpoint_1, $backup_id_1, $initiated_1, $created_at_1, $operation_id_1, $message_1)`
	)
	opId := types.GenerateObjectID()
	backupId := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		Id:          opId,
		ContainerID: "a",
		BackupId:    backupId,
		State:       "PENDING",
		Message:     "Message",
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     "endpoint",
			DatabaseName: "dbname",
		},
		YdbOperationId:      "1234",
		SourcePaths:         nil,
		SourcePathToExclude: nil,
		CreatedAt:           time.Unix(0, 0),
	}
	backup := types.Backup{
		ID:     backupId,
		Status: "Available",
	}
	builder := NewWriteTableQuery().
		WithUpdateBackup(backup).
		WithCreateOperation(&tbOp)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$id_0", table_types.UUIDValue(backupId)),
			table.ValueParam("$status_0", table_types.StringValueFromString("Available")),
			table.ValueParam("$id_1", table_types.UUIDValue(opId)),
			table.ValueParam("$type_1", table_types.StringValueFromString("TB")),
			table.ValueParam(
				"$status_1", table_types.StringValueFromString(string(tbOp.State)),
			),
			table.ValueParam(
				"$container_id_1", table_types.StringValueFromString(tbOp.ContainerID),
			),
			table.ValueParam(
				"$database_1",
				table_types.StringValueFromString(tbOp.YdbConnectionParams.DatabaseName),
			),
			table.ValueParam(
				"$endpoint_1",
				table_types.StringValueFromString(tbOp.YdbConnectionParams.Endpoint),
			),
			table.ValueParam(
				"$backup_id_1",
				table_types.UUIDValue(tbOp.BackupId),
			),
			table.ValueParam(
				"$initiated_1",
				table_types.StringValueFromString(""),
			),
			table.ValueParam(
				"$created_at_1",
				table_types.TimestampValueFromTime(tbOp.CreatedAt),
			),
			table.ValueParam(
				"$operation_id_1",
				table_types.StringValueFromString(tbOp.YdbOperationId),
			),
			table.ValueParam(
				"$message_1",
				table_types.StringValueFromString(tbOp.Message),
			),
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
