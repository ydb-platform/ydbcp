package handlers

import (
	"context"
	"fmt"
	"testing"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	s3Client "ydbcp/internal/connectors/s3"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTBOperationHandlerInvalidOperationResponse(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateRunning,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector()
	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)
	// try to handle tb operation with non-existing ydb operation id
	handler := NewTBOperationHandler(
		dbConnector,
		clientConnector,
		s3Connector,
		config.Config{},
		queries.NewWriteTableQueryMock,
	)
	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Error status: NOT_FOUND, issues: message:\"operation not found\"", op.GetMessage())
}

func TestTBOperationHandlerDeadlineExceededForRunningOperation(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateRunning,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 0,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	// check operation status (cancelling should be started because of deadline exceeded)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateStartCancelling, op.GetState())
	assert.Equal(t, "Operation deadline exceeded", op.GetMessage())

	// check backup status (should be the same as before because cancellation wasn't completed)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateRunning, b.Status)

	// check ydb operation status (should be the same as before because cancellation wasn't completed)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
}

func TestTBOperationHandlerRunningOperationInProgress(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateRunning,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	// check operation status (should be pending)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateRunning, op.GetState())
	assert.Equal(t, "", op.GetMessage())

	// check backup status (should be in pending)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateRunning, b.Status)

	// check ydb operation status (should be in progress)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestTBOperationHandlerRunningOperationCompletedSuccessfully(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:           backupID,
		Status:       types.BackupStateRunning,
		S3PathPrefix: "pathPrefix",
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
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
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())

	// check backup status and size (should be done)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateAvailable, b.Status)
	assert.Equal(t, int64(450), b.Size)

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestTBOperationHandlerRunningOperationCancelled(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateRunning,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_CANCELLED,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	// check operation status (should be error)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())

	// check backup status (should be error)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateError, b.Status)

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestTBOperationHandlerDeadlineExceededForCancellingOperation(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateRunning,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 0,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	// check operation status (should be failed because of deadline exceeded)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateError, op.GetState())
	assert.Equal(t, "Operation deadline exceeded", op.GetMessage())

	// check backup status (should be error)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateError, b.Status)

	// check ydb operation status (should be in progress)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestTBOperationHandlerCancellingOperationInProgress(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateRunning,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	// check operation status (should be the same as before)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelling, op.GetState())

	// check backup status (should be pending)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateRunning, b.Status)

	// check ydb operation status (should be in progress)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_SUCCESS, ydbOpStatus.GetOperation().GetStatus())
	assert.Equal(t, false, ydbOpStatus.GetOperation().GetReady())
}

func TestTBOperationHandlerCancellingOperationCompletedSuccessfully(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateCancelling,
		Message:             "operation was cancelled by user",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:           backupID,
		Status:       types.BackupStateRunning,
		S3PathPrefix: "pathPrefix",
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
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
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)
	// check operation status (should be done)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateDone, op.GetState())
	fmt.Print(op.GetMessage())
	assert.Equal(t, "Operation was completed despite cancellation: operation was cancelled by user", op.GetMessage())

	// check backup status (should be available)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateAvailable, b.Status)

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())
}

func TestTBOperationHandlerCancellingOperationCancelled(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateCancelling,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateRunning,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  true,
		Status: Ydb.StatusIds_CANCELLED,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	// check operation status (should be cancelled)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateCancelled, op.GetState())

	// check backup status (should be cancelled)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateCancelled, b.Status)

	// check ydb operation status (should be forgotten)
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_NOT_FOUND, ydbOpStatus.GetOperation().GetStatus())

}

func TestTBOperationHandlerRetriableErrorForRunningOperation(t *testing.T) {
	ctx := context.Background()
	opId := types.GenerateObjectID()
	backupID := types.GenerateObjectID()
	tbOp := types.TakeBackupOperation{
		ID:                  opId,
		BackupID:            backupID,
		State:               types.OperationStateRunning,
		Message:             "",
		YdbConnectionParams: types.YdbConnectionParams{},
		YdbOperationId:      "1",
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
		},
	}
	backup := types.Backup{
		ID:     backupID,
		Status: types.BackupStateRunning,
	}

	ydbOp := &Ydb_Operations.Operation{
		Id:     "1",
		Ready:  false,
		Status: Ydb.StatusIds_UNAVAILABLE,
		Issues: nil,
	}

	opMap := make(map[string]types.Operation)
	backupMap := make(map[string]types.Backup)
	ydbOpMap := make(map[string]*Ydb_Operations.Operation)
	s3ObjectsMap := make(map[string][]*s3.Object)
	backupMap[backupID] = backup
	opMap[opId] = &tbOp
	ydbOpMap["1"] = ydbOp
	dbConnector := db.NewMockDBConnector(
		db.WithBackups(backupMap),
		db.WithOperations(opMap),
	)
	clientConnector := client.NewMockClientConnector(
		client.WithOperations(ydbOpMap),
	)

	s3Connector := s3Client.NewMockS3Connector(s3ObjectsMap)

	handler := NewTBOperationHandler(
		dbConnector, clientConnector, s3Connector, config.Config{
			OperationTtlSeconds: 10000,
		}, queries.NewWriteTableQueryMock,
	)

	err := handler(ctx, tbOp.Copy())
	assert.Empty(t, err)

	// check operation status (should be the same as before)
	op, err := dbConnector.GetOperation(ctx, tbOp.ID)
	assert.Empty(t, err)
	assert.NotEmpty(t, op)
	assert.Equal(t, types.OperationStateRunning, op.GetState())

	// check backup status (should be pending)
	b, err := dbConnector.GetBackup(ctx, backupID)
	assert.Empty(t, err)
	assert.NotEmpty(t, b)
	assert.Equal(t, types.BackupStateRunning, b.Status)

	// check ydb operation status
	ydbOpStatus, err := clientConnector.GetOperationStatus(ctx, nil, tbOp.YdbOperationId)
	assert.Empty(t, err)
	assert.Equal(t, Ydb.StatusIds_UNAVAILABLE, ydbOpStatus.GetOperation().GetStatus())
}
