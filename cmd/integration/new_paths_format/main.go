package main

import (
	"context"
	"log"
	"time"
	"ydbcp/cmd/integration/common"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/grpc"
)

const (
	containerID      = "abcde"
	databaseName     = "/local"
	ydbcpEndpoint    = "0.0.0.0:50051"
	databaseEndpoint = "grpcs://local-ydb:2135"
)

type backupScenario struct {
	name                string
	request             *pb.MakeBackupRequest
	expectedRootPath    string
	expectedSourcePaths []string
}

type negativeBackupScenario struct {
	name    string
	request *pb.MakeBackupRequest
}

func newMakeBackupRequest(rootPath string, sourcePaths []string) *pb.MakeBackupRequest {
	return &pb.MakeBackupRequest{
		ContainerId:      containerID,
		DatabaseName:     databaseName,
		DatabaseEndpoint: databaseEndpoint,
		RootPath:         rootPath,
		SourcePaths:      sourcePaths,
	}
}

func runBackupScenario(ctx context.Context, backupClient pb.BackupServiceClient, opClient pb.OperationServiceClient, scenario backupScenario) {
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

	if op.GetRootPath() != scenario.expectedRootPath {
		log.Panicf("scenario %s: expected root path %q, got %q", scenario.name, scenario.expectedRootPath, op.GetRootPath())
	}

	if !equalStringSlices(op.GetSourcePaths(), scenario.expectedSourcePaths) {
		log.Panicf("scenario %s: expected source paths %v, got %v", scenario.name, scenario.expectedSourcePaths, op.GetSourcePaths())
	}

	log.Printf("scenario %s: passed", scenario.name)
}

func runNegativeBackupScenario(ctx context.Context, backupClient pb.BackupServiceClient, opClient pb.OperationServiceClient, scenario negativeBackupScenario) {
	// MakeBackup should succeed and return an operation
	tbwr, err := backupClient.MakeBackup(ctx, scenario.request)
	if err != nil {
		log.Panicf("scenario %s: MakeBackup should succeed but got error: %v", scenario.name, err)
	}

	operationID := tbwr.Id
	log.Printf("scenario %s: operation created with ID %s, waiting for it to fail...", scenario.name, operationID)

	// Wait for the operation to process and fail
	maxWait := 10 * time.Second
	deadline := time.Now().Add(maxWait)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var op *pb.Operation
	for time.Now().Before(deadline) {
		<-ticker.C
		op, err = opClient.GetOperation(ctx, &pb.GetOperationRequest{Id: operationID})
		if err != nil {
			log.Panicf("scenario %s: failed to get operation: %v", scenario.name, err)
		}

		// Check if operation is in ERROR state
		if op.GetStatus() == pb.Operation_ERROR {

			log.Printf("scenario %s: passed", scenario.name)
			return
		}

		// If operation completed successfully, that's unexpected
		if op.GetStatus() == pb.Operation_DONE {
			log.Panicf("scenario %s: operation completed successfully but expected ERROR status", scenario.name)
		}
	}

	// Timeout - operation didn't reach ERROR state
	log.Panicf("scenario %s: timeout waiting for operation to reach ERROR status. Current status: %v, message: %q", scenario.name, op.GetStatus(), op.GetMessage())
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

	scenarios := []backupScenario{
		{
			name:                "full backup",
			request:             newMakeBackupRequest("", nil),
			expectedRootPath:    "",
			expectedSourcePaths: []string{},
		},
		{
			name:                "full backup with specified root path",
			request:             newMakeBackupRequest("stocks", nil),
			expectedRootPath:    "stocks",
			expectedSourcePaths: []string{},
		},
		{
			name:                "partial backup",
			request:             newMakeBackupRequest("", []string{"kv_test"}),
			expectedRootPath:    "",
			expectedSourcePaths: []string{"kv_test"},
		},
		{
			name:                "partial backup with specified root path",
			request:             newMakeBackupRequest("stocks", []string{"orders", "orderLines"}),
			expectedRootPath:    "stocks",
			expectedSourcePaths: []string{"orders", "orderLines"},
		},
	}

	for _, scenario := range scenarios {
		runBackupScenario(ctx, backupClient, opClient, scenario)
		time.Sleep(2 * time.Second)
	}

	negativeScenarios := []negativeBackupScenario{
		{
			name:    "non-existing root path",
			request: newMakeBackupRequest("non_existing_path", nil),
		},
		{
			name:    "non-existing source path",
			request: newMakeBackupRequest("", []string{"non_existing_table"}),
		},
		{
			name:    "non-existing source paths with root path",
			request: newMakeBackupRequest("stocks", []string{"non_existing_table1", "non_existing_table2"}),
		},
		{
			name:    "mixed existing and non-existing source paths",
			request: newMakeBackupRequest("", []string{"kv_test", "non_existing_table"}),
		},
		{
			name:    "absolute root path",
			request: newMakeBackupRequest("/local/stocks", nil),
		},
		{
			name:    "absolute sorce path",
			request: newMakeBackupRequest("", []string{"/local/stocks/orders"}),
		},
		{
			name:    "absolute source path with root path",
			request: newMakeBackupRequest("stocks", []string{"/local/stocks/orders"}),
		},
		{
			name:    "source path relative to db root with root path",
			request: newMakeBackupRequest("stocks", []string{"stocks/orders"}),
		},
	}

	for _, scenario := range negativeScenarios {
		runNegativeBackupScenario(ctx, backupClient, opClient, scenario)
	}
}
