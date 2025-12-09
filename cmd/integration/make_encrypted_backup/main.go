package main

import (
	"context"
	"log"
	"time"
	"ydbcp/cmd/integration/common"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	containerID      = "abcde"
	databaseName     = "/local"
	ydbcpEndpoint    = "0.0.0.0:50051"
	databaseEndpoint = "grpcs://local-ydb:2135"
	testKmsKeyID     = "test-kms-key-id-123"
)

type encryptedBackupScenario struct {
	name    string
	request *pb.MakeBackupRequest
}

type negativeEncryptedBackupScenario struct {
	name           string
	request        *pb.MakeBackupRequest
	expectedStatus codes.Code
}

func newEncryptedBackupRequest(rootPath string, sourcePaths []string, kmsKeyID string) *pb.MakeBackupRequest {
	encryptionSettings := &pb.EncryptionSettings{
		Algorithm: pb.EncryptionSettings_AES_256_GCM,
	}

	if len(kmsKeyID) > 0 {
		encryptionSettings.KeyEncryptionKey = &pb.EncryptionSettings_KmsKey_{
			KmsKey: &pb.EncryptionSettings_KmsKey{
				KeyId: kmsKeyID,
			},
		}
	}

	return &pb.MakeBackupRequest{
		ContainerId:        containerID,
		DatabaseName:       databaseName,
		DatabaseEndpoint:   databaseEndpoint,
		RootPath:           rootPath,
		SourcePaths:        sourcePaths,
		EncryptionSettings: encryptionSettings,
	}
}

func runEncryptedBackupScenario(ctx context.Context, backupClient pb.BackupServiceClient, opClient pb.OperationServiceClient, scenario encryptedBackupScenario) {
	log.Printf("Running scenario: %s", scenario.name)

	tbwr, err := backupClient.MakeBackup(ctx, scenario.request)
	if err != nil {
		log.Panicf("scenario %s: failed to make backup: %v", scenario.name, err)
	}

	op, err := opClient.GetOperation(
		ctx, &pb.GetOperationRequest{
			Id: tbwr.Id,
		},
	)
	if err != nil {
		log.Panicf("scenario %s: failed to get operation: %v", scenario.name, err)
	}

	if op.EncryptionSettings == nil {
		log.Panicf("scenario %s: encryption settings are nil", scenario.name)
	}

	if op.EncryptionSettings.GetKmsKey() == nil {
		log.Panicf("scenario %s: KMS key is nil", scenario.name)
	}

	if op.EncryptionSettings.GetKmsKey().GetKeyId() != testKmsKeyID {
		log.Panicf("scenario %s: KMS key ID mismatch, expected %s, got %s",
			scenario.name, testKmsKeyID, op.EncryptionSettings.GetKmsKey().GetKeyId())
	}

	if op.EncryptionSettings.GetAlgorithm() != pb.EncryptionSettings_AES_256_GCM {
		log.Panicf("scenario %s: encryption algorithm is not AES_256_GCM", scenario.name)
	}

	// Wait for operation handler
	time.Sleep(time.Second * 3)

	ops, err := opClient.ListOperations(
		ctx, &pb.ListOperationsRequest{
			ContainerId:      containerID,
			DatabaseNameMask: databaseName,
			OperationTypes:   []string{types.OperationTypeTB.String()},
		},
	)
	if err != nil {
		log.Panicf("failed to list operations: %v", err)
	}

	var tbOperation *pb.Operation
	for _, op := range ops.Operations {
		if op.GetParentOperationId() == tbwr.Id {
			tbOperation = op
			break
		}
	}

	if tbOperation == nil {
		log.Panicf("scenario %s: TB operation not found", scenario.name)
	}

	// Wait for backup to complete
	done := false
	var backup *pb.Backup
	for range 30 {
		backup, err = backupClient.GetBackup(
			ctx,
			&pb.GetBackupRequest{Id: tbOperation.BackupId},
		)
		if err != nil {
			log.Panicf("scenario %s: failed to get backup: %v", scenario.name, err)
		}
		if backup.GetStatus().String() == types.BackupStateAvailable {
			done = true
			break
		}
		time.Sleep(time.Second)
	}
	if !done {
		log.Panicf("scenario %s: failed to complete backup in 30 seconds", scenario.name)
	}

	// Verify the backup has encryption settings
	if backup.EncryptionSettings == nil {
		log.Panicf("scenario %s: backup should have encryption settings", scenario.name)
	}

	if backup.EncryptionSettings.GetKmsKey() == nil {
		log.Panicf("scenario %s: backup should have KMS key", scenario.name)
	}

	if backup.EncryptionSettings.GetKmsKey().GetKeyId() != testKmsKeyID {
		log.Panicf("scenario %s: KMS key ID mismatch, expected %s, got %s",
			scenario.name, testKmsKeyID, backup.EncryptionSettings.GetKmsKey().GetKeyId())
	}

	if backup.EncryptionSettings.GetAlgorithm() != pb.EncryptionSettings_AES_256_GCM {
		log.Panicf("scenario %s: encryption algorithm is not AES_256_GCM", scenario.name)
	}

	restoreRequest := &pb.MakeRestoreRequest{
		ContainerId:      containerID,
		BackupId:         backup.Id,
		DatabaseName:     databaseName,
		DatabaseEndpoint: databaseEndpoint,
		DestinationPath:  "/restored_backup_" + backup.Id,
		SourcePaths:      scenario.request.SourcePaths,
	}

	restoreOperation, err := backupClient.MakeRestore(ctx, restoreRequest)
	if err != nil {
		log.Panicf("scenario %s: failed to make restore: %v", scenario.name, err)
	}

	// Wait for restore operation to complete
	done = false
	for range 30 {
		op, err := opClient.GetOperation(
			ctx,
			&pb.GetOperationRequest{Id: restoreOperation.Id},
		)
		if err != nil {
			log.Panicf("scenario %s: failed to get restore operation: %v", scenario.name, err)
		}
		if op.GetStatus().String() == types.OperationStateDone.String() {
			done = true
			break
		}
		time.Sleep(time.Second)
	}
	if !done {
		log.Panicf("scenario %s: failed to complete restore in 30 seconds", scenario.name)
	}

	log.Printf("scenario %s: passed", scenario.name)
}

func runNegativeEncryptedBackupScenario(ctx context.Context, backupClient pb.BackupServiceClient, opClient pb.OperationServiceClient, scenario negativeEncryptedBackupScenario) {
	log.Printf("Running negative scenario: %s", scenario.name)

	_, err := backupClient.MakeBackup(ctx, scenario.request)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			log.Panicf("scenario %s: MakeBackup failed but couldn't extract status: %v", scenario.name, err)
		}
		if st.Code() != scenario.expectedStatus {
			log.Panicf("scenario %s: expected status code %v, got %v: %v", scenario.name, scenario.expectedStatus, st.Code(), err)
		}
		log.Printf("scenario %s: passed", scenario.name)
	} else {
		log.Panicf("scenario %s: MakeBackup should fail with status code %v, but it succeeded", scenario.name, scenario.expectedStatus)
	}
}

func main() {
	conn := common.CreateGRPCClient(ydbcpEndpoint)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Panicln("failed to close connection")
		}
	}(conn)
	backupClient := pb.NewBackupServiceClient(conn)
	opClient := pb.NewOperationServiceClient(conn)

	ctx := context.Background()

	positiveScenarios := []encryptedBackupScenario{
		{
			name:    "full encrypted backup",
			request: newEncryptedBackupRequest("", nil, testKmsKeyID),
		},
		{
			name:    "partial encrypted backup",
			request: newEncryptedBackupRequest("", []string{"kv_test"}, testKmsKeyID),
		},
		{
			name:    "full encrypted backup with root path",
			request: newEncryptedBackupRequest("stocks", nil, testKmsKeyID),
		},
		{
			name:    "partial encrypted backup with root path",
			request: newEncryptedBackupRequest("stocks", []string{"orders"}, testKmsKeyID),
		},
	}

	for _, scenario := range positiveScenarios {
		runEncryptedBackupScenario(ctx, backupClient, opClient, scenario)
		time.Sleep(time.Second)
	}

	negativeScenarios := []negativeEncryptedBackupScenario{
		{
			name:           "encrypted backup with empty kms key id",
			request:        newEncryptedBackupRequest("", nil, ""),
			expectedStatus: codes.InvalidArgument,
		},
	}

	for _, scenario := range negativeScenarios {
		runNegativeEncryptedBackupScenario(ctx, backupClient, opClient, scenario)
	}
}
