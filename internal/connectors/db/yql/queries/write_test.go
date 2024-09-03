package queries

import (
	"context"
	"strings"
	"testing"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/protobuf/types/known/timestamppb"

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
		ID:      opId,
		State:   "Done",
		Message: "Abcde",
	}
	backup := types.Backup{
		ID:     backupId,
		Status: "Available",
	}
	builder := NewWriteTableQuery(context.Background()).
		WithUpdateBackup(backup).
		WithUpdateOperation(&op)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$id_0", table_types.StringValueFromString(backupId)),
			table.ValueParam("$status_0", table_types.StringValueFromString("Available")),
			table.ValueParam("$id_1", table_types.StringValueFromString(opId)),
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
		queryString = `UPSERT INTO Backups (id, container_id, database, endpoint, s3_endpoint, s3_region, s3_bucket, s3_path_prefix, status, message, size, initiated, created_at) VALUES ($id_0, $container_id_0, $database_0, $endpoint_0, $s3_endpoint_0, $s3_region_0, $s3_bucket_0, $s3_path_prefix_0, $status_0, $message_0, $size_0, $initiated_0, $created_at_0);
UPSERT INTO Operations (id, type, status, message, initiated, created_at, container_id, database, endpoint, backup_id, operation_id) VALUES ($id_1, $type_1, $status_1, $message_1, $initiated_1, $created_at_1, $container_id_1, $database_1, $endpoint_1, $backup_id_1, $operation_id_1)`
	)
	opId := types.GenerateObjectID()
	backupId := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:          opId,
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
		Audit: &pb.AuditInfo{
			Creator:   "author",
			CreatedAt: timestamppb.Now(),
		},
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
		AuditInfo: &pb.AuditInfo{
			Creator:   "author",
			CreatedAt: timestamppb.Now(),
		},
	}
	builder := NewWriteTableQuery(context.Background()).
		WithCreateBackup(backup).
		WithCreateOperation(&tbOp)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$id_0", table_types.StringValueFromString(backupId)),
			table.ValueParam("$container_id_0", table_types.StringValueFromString("a")),
			table.ValueParam("$database_0", table_types.StringValueFromString("b")),
			table.ValueParam("$endpoint_0", table_types.StringValueFromString("g")),
			table.ValueParam("$s3_endpoint_0", table_types.StringValueFromString("c")),
			table.ValueParam("$s3_region_0", table_types.StringValueFromString("d")),
			table.ValueParam("$s3_bucket_0", table_types.StringValueFromString("e")),
			table.ValueParam("$s3_path_prefix_0", table_types.StringValueFromString("f")),
			table.ValueParam("$status_0", table_types.StringValueFromString("Available")),
			table.ValueParam("$message_0", table_types.StringValueFromString("msg backup")),
			table.ValueParam("$size_0", table_types.Int64Value(0)),
			table.ValueParam("$initiated_0", table_types.StringValueFromString("author")),
			table.ValueParam(
				"$created_at_0",
				table_types.TimestampValueFromTime(backup.AuditInfo.CreatedAt.AsTime()),
			),

			table.ValueParam("$id_1", table_types.StringValueFromString(opId)),
			table.ValueParam("$type_1", table_types.StringValueFromString("TB")),
			table.ValueParam(
				"$status_1", table_types.StringValueFromString(string(tbOp.State)),
			),
			table.ValueParam("$message_1", table_types.StringValueFromString("msg op")),
			table.ValueParam(
				"$initiated_1",
				table_types.StringValueFromString("author"),
			),
			table.ValueParam(
				"$created_at_1",
				table_types.TimestampValueFromTime(tbOp.Audit.CreatedAt.AsTime()),
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
				table_types.StringValueFromString(tbOp.BackupId),
			),
			table.ValueParam(
				"$operation_id_1",
				table_types.StringValueFromString(tbOp.YdbOperationId),
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

func TestQueryBuilder_UpdateCreate(t *testing.T) {
	const (
		queryString = `UPDATE Backups SET status = $status_0 WHERE id = $id_0;
UPSERT INTO Operations (id, type, status, message, initiated, created_at, container_id, database, endpoint, backup_id, operation_id, paths, paths_to_exclude) VALUES ($id_1, $type_1, $status_1, $message_1, $initiated_1, $created_at_1, $container_id_1, $database_1, $endpoint_1, $backup_id_1, $operation_id_1, $paths_1, $paths_to_exclude_1)`
	)
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupId := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:          opId,
		ContainerID: "a",
		BackupId:    backupId,
		State:       "PENDING",
		Message:     "Message",
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     "endpoint",
			DatabaseName: "dbname",
		},
		YdbOperationId:      "1234",
		SourcePaths:         []string{"path"},
		SourcePathToExclude: []string{"exclude1", "exclude2"},
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupId,
		Status: "Available",
	}
	builder := NewWriteTableQuery(ctx).
		WithUpdateBackup(backup).
		WithCreateOperation(&tbOp)
	var (
		queryParams = table.NewQueryParameters(
			table.ValueParam("$id_0", table_types.StringValueFromString(backupId)),
			table.ValueParam("$status_0", table_types.StringValueFromString("Available")),
			table.ValueParam("$id_1", table_types.StringValueFromString(opId)),
			table.ValueParam("$type_1", table_types.StringValueFromString("TB")),
			table.ValueParam(
				"$status_1", table_types.StringValueFromString(string(tbOp.State)),
			),
			table.ValueParam(
				"$message_1",
				table_types.StringValueFromString(tbOp.Message),
			),
			table.ValueParam(
				"$initiated_1",
				table_types.StringValueFromString(""),
			),
			table.ValueParam(
				"$created_at_1",
				table_types.TimestampValueFromTime(tbOp.Audit.CreatedAt.AsTime()),
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
				table_types.StringValueFromString(tbOp.BackupId),
			),
			table.ValueParam(
				"$operation_id_1",
				table_types.StringValueFromString(tbOp.YdbOperationId),
			),
			table.ValueParam(
				"$paths_1",
				table_types.StringValueFromString(strings.Join(tbOp.SourcePaths, ",")),
			),
			table.ValueParam(
				"$paths_to_exclude_1",
				table_types.StringValueFromString(strings.Join(tbOp.SourcePathToExclude, ",")),
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
