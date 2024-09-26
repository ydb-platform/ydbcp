package handlers

import (
	"context"
	"testing"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	s3Client "ydbcp/internal/connectors/s3"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDBOperationHandlerDeadlineExceededForRunningOperation(t *testing.T) {
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
	s3ObjectsMap := make(map[string][]*s3.Object)
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

	err := handler(ctx, dbOp.Copy())
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
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &dbOp
	s3ObjectsMap["pathPrefix"] = []*s3.Object{
		{
			Key:  aws.String("data_1.csv"),
			Size: aws.Int64(100),
		},
		{
			Key:  aws.String("data_2.csv"),
			Size: aws.Int64(150),
		},
		{
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

	err := handler(ctx, dbOp.Copy())
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
	objects, err := s3Connector.ListObjects("pathPrefix", "bucket")
	assert.Empty(t, err)
	assert.Empty(t, objects)
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
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &dbOp
	s3ObjectsMap["pathPrefix"] = []*s3.Object{
		{
			Key:  aws.String("data_1.csv"),
			Size: aws.Int64(100),
		},
		{
			Key:  aws.String("data_2.csv"),
			Size: aws.Int64(150),
		},
		{
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

	err := handler(ctx, dbOp.Copy())
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
	objects, err := s3Connector.ListObjects("pathPrefix", "bucket")
	assert.Empty(t, err)
	assert.Empty(t, objects)
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
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &dbOp
	s3ObjectsMap["pathPrefix"] = []*s3.Object{
		{
			Key:  aws.String("data_1.csv"),
			Size: aws.Int64(100),
		},
		{
			Key:  aws.String("data_2.csv"),
			Size: aws.Int64(150),
		},
		{
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

	err := handler(ctx, dbOp.Copy())
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
