package handlers

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"testing"
	"ydbcp/internal/metrics"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	s3Client "ydbcp/internal/connectors/s3"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDBOperationHandlerDeadlineExceededForRunningOperation(t *testing.T) {
	metrics.InitializeMockMetricsRegistry()
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	dbOp := types.DeleteBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateDeleting,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	s3ObjectsMap := make(map[string]s3Client.Bucket)
	backupMap[backupID] = backup
	opMap[opId] = &dbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewDBOperationHandler(
		dbConnector, s3Connector, config.Config{
			OperationTtlSeconds: 0,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &dbOp)
	assert.Empty(t, err)

	// check operation status (should be error because of deadline exceeded)
	op, err := dbConnector.GetOperation(ctx, dbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Operation deadline exceeded", op.GetMessage())

	// check backup status (should be error)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateError, b.Status)
}

func TestDBOperationHandlerPendingOperationCompletedSuccessfully(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	dbOp := types.DeleteBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStatePending,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:           backupID,
		Status:       types.BackupStateDeleting,
		S3PathPrefix: "pathPrefix",
		S3Bucket:     "bucket",
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	s3ObjectsMap := make(map[string]s3Client.Bucket)
	backupMap[backupID] = backup
	opMap[opId] = &dbOp
	s3ObjectsMap["bucket"] = s3Client.Bucket{
		"pathPrefix/data_1.csv": {
			Key:  aws.String("data_1.csv"),
			Size: aws.Int64(100),
		},
		"pathPrefix/data_2.csv": {
			Key:  aws.String("data_2.csv"),
			Size: aws.Int64(150),
		},
		"pathPrefix/data_3.csv": {
			Key:  aws.String("data_3.csv"),
			Size: aws.Int64(200),
		},
	}

	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	metrics.InitializeMockMetricsRegistry()
	handler := NewDBOperationHandler(
		dbConnector, s3Connector, config.Config{
			OperationTtlSeconds: 1000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &dbOp)
	assert.Empty(t, err)

	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, dbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())
	assert.Equal(t, "Success", op.GetMessage())

	// check backup status (should be deleted)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateDeleted, b.Status)

	// check s3 objects (should be deleted)
	objects, _, err := s3Connector.ListObjects("pathPrefix", "bucket")
	assert.Empty(t, err)
	assert.Empty(t, objects)

	// check metrics
	assert.Equal(t, float64(450), metrics.GetMetrics()["storage_bytes_deleted"])
}

func TestDBOperationHandlerRunningOperationCompletedSuccessfully(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	dbOp := types.DeleteBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:           backupID,
		Status:       types.BackupStateDeleting,
		S3PathPrefix: "pathPrefix",
		S3Bucket:     "bucket",
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	s3ObjectsMap := make(map[string]s3Client.Bucket)
	backupMap[backupID] = backup
	opMap[opId] = &dbOp
	s3ObjectsMap["bucket"] = s3Client.Bucket{
		"pathPrefix/data_1.csv": {
			Key:  aws.String("data_1.csv"),
			Size: aws.Int64(100),
		},
		"pathPrefix/data_2.csv": {
			Key:  aws.String("data_2.csv"),
			Size: aws.Int64(150),
		},
		"pathPrefix/data_3.csv": {
			Key:  aws.String("data_3.csv"),
			Size: aws.Int64(200),
		},
	}

	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	metrics.InitializeMockMetricsRegistry()
	handler := NewDBOperationHandler(
		dbConnector, s3Connector, config.Config{
			OperationTtlSeconds: 1000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &dbOp)
	assert.Empty(t, err)

	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, dbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())
	assert.Equal(t, "Success", op.GetMessage())

	// check backup status (should be deleted)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateDeleted, b.Status)

	// check s3 objects (should be deleted)
	objects, _, err := s3Connector.ListObjects("pathPrefix", "bucket")
	assert.Empty(t, err)
	assert.Empty(t, objects)

	// check metrics
	assert.Equal(t, float64(450), metrics.GetMetrics()["storage_bytes_deleted"])
}

func TestDBOperationHandlerUnexpectedBackupStatus(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	dbOp := types.DeleteBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:           backupID,
		Status:       types.BackupStateAvailable,
		S3PathPrefix: "pathPrefix",
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	s3ObjectsMap := make(map[string]s3Client.Bucket)
	backupMap[backupID] = backup
	opMap[opId] = &dbOp
	s3ObjectsMap["bucket"] = s3Client.Bucket{
		"pathPrefix/data_1.csv": {
			Key:  aws.String("data_1.csv"),
			Size: aws.Int64(100),
		},
		"pathPrefix/data_2.csv": {
			Key:  aws.String("data_2.csv"),
			Size: aws.Int64(150),
		},
		"pathPrefix/data_3.csv": {
			Key:  aws.String("data_3.csv"),
			Size: aws.Int64(200),
		},
	}

	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewDBOperationHandler(
		dbConnector, s3Connector, config.Config{
			OperationTtlSeconds: 1000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &dbOp)
	assert.Empty(t, err)

	// check operation status (should be failed)
	op, err := dbConnector.GetOperation(ctx, dbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Unexpected backup status: AVAILABLE", op.GetMessage())

	// check backup status (should be the same as before)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateAvailable, b.Status)
}

func TestDBOperationHandlerDeleteMoreThanAllowedLimit(t *testing.T) {
	const objectsListSize = 1005

	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	dbOp := types.DeleteBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:           backupID,
		Status:       types.BackupStateDeleting,
		S3PathPrefix: "pathPrefix",
		S3Bucket:     "bucket",
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	s3ObjectsMap := make(map[string]s3Client.Bucket)
	backupMap[backupID] = backup
	opMap[opId] = &dbOp

	s3ObjectsMap["bucket"] = make(s3Client.Bucket)
	for i := 0; i < objectsListSize; i++ {
		bucket := s3ObjectsMap["bucket"]

		bucket[fmt.Sprintf("pathPrefix/data_%d.csv", i)] = &s3.Object{
			Key:  aws.String(fmt.Sprintf("data_%d.csv", i)),
			Size: aws.Int64(10),
		}
	}

	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	metrics.InitializeMockMetricsRegistry()
	handler := NewDBOperationHandler(
		dbConnector, s3Connector, config.Config{
			OperationTtlSeconds: 1000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, &dbOp)
	assert.Empty(t, err)

	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, dbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())
	assert.Equal(t, "Success", op.GetMessage())

	// check backup status (should be deleted)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateDeleted, b.Status)

	// check s3 objects (should be deleted)
	objects, _, err := s3Connector.ListObjects("pathPrefix", "bucket")
	assert.Empty(t, err)
	assert.Empty(t, objects)

	// check metrics
	assert.Equal(t, float64(objectsListSize*10), metrics.GetMetrics()["storage_bytes_deleted"])
}
