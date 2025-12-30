package main

import (
	"context"
	"log"
	"os"
	"slices"
	"time"
	"ydbcp/cmd/integration/common"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Import_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Import"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	containerID      = "abcde"
	databaseName     = "/local"
	ydbcpEndpoint    = "0.0.0.0:50051"
	databaseEndpoint = "grpcs://local-ydb:2135"
)

type backupScenario struct {
	name                 string
	request              *pb.MakeBackupRequest
	expectedRootPath     string
	expectedSourcePaths  []string
	sourcePathsToExclude []string
}

type negativeBackupScenario struct {
	name    string
	request *pb.MakeBackupRequest
}

func newMakeBackupRequest(rootPath string, sourcePaths []string, sourcePathsToExclude []string) *pb.MakeBackupRequest {
	return &pb.MakeBackupRequest{
		ContainerId:          containerID,
		DatabaseName:         databaseName,
		DatabaseEndpoint:     databaseEndpoint,
		RootPath:             rootPath,
		SourcePaths:          sourcePaths,
		SourcePathsToExclude: sourcePathsToExclude,
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

	actualSourcePathsToExclude := op.GetSourcePathsToExclude()
	slices.Sort(actualSourcePathsToExclude)
	slices.Sort(scenario.sourcePathsToExclude)

	if !slices.Equal(actualSourcePathsToExclude, scenario.sourcePathsToExclude) {
		log.Panicf("scenario %s: expected source paths to exclude %v, got %v", scenario.name, scenario.sourcePathsToExclude, op.GetSourcePathsToExclude())
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

	ydbDriver := common.OpenYdb(databaseEndpoint, databaseName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	importClient := Ydb_Import_V1.NewImportServiceClient(ydb.GRPCConn(ydbDriver))
	listObjectsRequest := &Ydb_Import.ListObjectsInS3ExportRequest{
		OperationParams: &Ydb_Operations.OperationParams{
			OperationTimeout: durationpb.New(time.Second),
			CancelAfter:      durationpb.New(time.Second),
		},
		Settings: &Ydb_Import.ListObjectsInS3ExportSettings{
			Endpoint:  backup.GetLocation().GetEndpoint(),
			Region:    backup.GetLocation().GetRegion(),
			AccessKey: os.Getenv("S3_ACCESS_KEY"),
			SecretKey: os.Getenv("S3_SECRET_KEY"),
			Bucket:    backup.GetLocation().GetBucket(),
			Prefix:    backup.GetLocation().GetPathPrefix(),
			Scheme:    Ydb_Import.ImportFromS3Settings_HTTPS,
		},
	}

	response, err := importClient.ListObjectsInS3Export(ctx, listObjectsRequest)
	if err != nil {
		log.Panicf("scenario %s: failed to list objects: %v", scenario.name, err)
	}

	if response.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS {
		log.Panicf("scenario %s: listing objects in S3 export failed: %v", scenario.name, response.GetOperation().GetIssues())
	}

	operation := response.GetOperation()
	if operation.GetResult() == nil {
		log.Printf("scenario %s: no objects found in backup", scenario.name)
	} else {
		var result Ydb_Import.ListObjectsInS3ExportResult
		if err := operation.GetResult().UnmarshalTo(&result); err != nil {
			log.Panicf("scenario %s: error unmarshaling operation result: %v", scenario.name, err)
		}

		items := result.GetItems()
		objects := make([]string, 0, len(items))
		for _, item := range items {
			if path := item.GetPath(); path != "" {
				objects = append(objects, path)
			} else if prefix := item.GetPrefix(); prefix != "" {
				objects = append(objects, prefix)
			}
		}

		slices.Sort(objects)
		slices.Sort(scenario.expectedSourcePaths)

		if !slices.Equal(objects, scenario.expectedSourcePaths) {
			log.Panicf("scenario %s: expected source paths %v, got %v", scenario.name, scenario.expectedSourcePaths, objects)
		}
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
			request:             newMakeBackupRequest("", nil, nil),
			expectedRootPath:    "",
			expectedSourcePaths: []string{"BackupSchedules", "Backups", "OperationTypes", "Operations", "goose_db_version", "kv_test", "stocks/orderLines", "stocks/orders", "stocks/stock"},
		},
		{
			name:                 "full backup with exclude filter",
			request:              newMakeBackupRequest("", nil, []string{".*Backup.*", ".*Operation.*"}),
			expectedRootPath:     "",
			expectedSourcePaths:  []string{"goose_db_version", "kv_test", "stocks/orderLines", "stocks/orders", "stocks/stock"},
			sourcePathsToExclude: []string{".*Backup.*", ".*Operation.*"},
		},
		{
			name:                "full backup with specified root path",
			request:             newMakeBackupRequest("stocks", nil, nil),
			expectedRootPath:    "stocks",
			expectedSourcePaths: []string{"orderLines", "orders", "stock"},
		},
		{
			name:                 "full backup with specified root path and exclude filter",
			request:              newMakeBackupRequest("stocks", nil, []string{".*order.*"}),
			expectedRootPath:     "stocks",
			expectedSourcePaths:  []string{"stock"},
			sourcePathsToExclude: []string{".*order.*"},
		},
		{
			name:                "partial backup",
			request:             newMakeBackupRequest("", []string{"kv_test"}, nil),
			expectedRootPath:    "",
			expectedSourcePaths: []string{"kv_test"},
		},
		{
			name:                "partial backup with specified root path",
			request:             newMakeBackupRequest("stocks", []string{"orders", "orderLines"}, nil),
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
			request: newMakeBackupRequest("non_existing_path", nil, nil),
		},
		{
			name:    "non-existing source path",
			request: newMakeBackupRequest("", []string{"non_existing_table"}, nil),
		},
		{
			name:    "non-existing source paths with root path",
			request: newMakeBackupRequest("stocks", []string{"non_existing_table1", "non_existing_table2"}, nil),
		},
		{
			name:    "mixed existing and non-existing source paths",
			request: newMakeBackupRequest("", []string{"kv_test", "non_existing_table"}, nil),
		},
		{
			name:    "absolute root path",
			request: newMakeBackupRequest("/local/stocks", nil, nil),
		},
		{
			name:    "absolute sorce path",
			request: newMakeBackupRequest("", []string{"/local/stocks/orders"}, nil),
		},
		{
			name:    "absolute source path with root path",
			request: newMakeBackupRequest("stocks", []string{"/local/stocks/orders"}, nil),
		},
		{
			name:    "source path relative to db root with root path",
			request: newMakeBackupRequest("stocks", []string{"stocks/orders"}, nil),
		},
	}

	for _, scenario := range negativeScenarios {
		runNegativeBackupScenario(ctx, backupClient, opClient, scenario)
	}
}
