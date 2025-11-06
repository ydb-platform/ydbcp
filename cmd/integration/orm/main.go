package main

import (
	"context"
	"log"
	"reflect"
	"time"
	"ydbcp/cmd/integration/common"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	containerID      = "abcde"
	databaseName     = "/local"
	ydbcpEndpoint    = "0.0.0.0:50051"
	connectionString = "grpcs://local-ydb:2135/local"
)

var (
	t1 = time.Date(2024, 01, 01, 00, 00, 00, 0, time.UTC)
	t2 = time.Date(2024, 01, 02, 00, 00, 00, 0, time.UTC)
)

func OperationsToInsert() []types.TakeBackupWithRetryOperation {
	schedule := "schedule"
	ttl := time.Hour * 24
	return []types.TakeBackupWithRetryOperation{
		{
			TakeBackupOperation: types.TakeBackupOperation{
				ID:          "1",
				ContainerID: containerID,
				State:       "xz",
				Message:     "nz",
				YdbConnectionParams: types.YdbConnectionParams{
					Endpoint:     ydbcpEndpoint,
					DatabaseName: databaseName,
				},
				RootPath:             "root",
				SourcePaths:          []string{"path"},
				SourcePathsToExclude: []string{"exclude"},
				Audit: &pb.AuditInfo{
					Creator:     "ydbcp",
					CreatedAt:   timestamppb.New(t1),
					CompletedAt: timestamppb.New(t2),
				},
				UpdatedAt: timestamppb.New(t2),
			},
			ScheduleID:  &schedule,
			Ttl:         &ttl,
			Retries:     0,
			RetryConfig: &pb.RetryConfig{Retries: &pb.RetryConfig_Count{Count: 5}},
		},
		{
			TakeBackupOperation: types.TakeBackupOperation{
				ID:          "1",
				ContainerID: containerID,
				State:       "xz",
				Message:     "nz",
				YdbConnectionParams: types.YdbConnectionParams{
					Endpoint:     ydbcpEndpoint,
					DatabaseName: databaseName,
				},
				RootPath:             "root",
				SourcePaths:          []string{"path"},
				SourcePathsToExclude: []string{"exclude"},
				Audit: &pb.AuditInfo{
					Creator:     "ydbcp",
					CreatedAt:   timestamppb.New(t1),
					CompletedAt: timestamppb.New(t2),
				},
				UpdatedAt: timestamppb.New(t2),
			},
			ScheduleID:  &schedule,
			Ttl:         &ttl,
			Retries:     0,
			RetryConfig: &pb.RetryConfig{Retries: &pb.RetryConfig_MaxBackoff{MaxBackoff: durationpb.New(ttl)}},
		},
	}
}

func ReadTBWROperation(ctx context.Context, ydbConn *db.YdbConnector, id string) *types.TakeBackupWithRetryOperation {
	operations, err := ydbConn.SelectOperations(ctx, queries.NewReadTableQuery(
		queries.WithTableName("Operations"),
		queries.WithQueryFilters(queries.QueryFilter{
			Field:  "id",
			Values: []table_types.Value{table_types.StringValueFromString(id)},
		}),
	))
	if err != nil {
		log.Panicf("failed to select operations: %v", err)
	}
	if len(operations) != 1 {
		log.Panicf("expected 1 operation, got %d", len(operations))
	}
	return operations[0].(*types.TakeBackupWithRetryOperation)
}

func TestTBWROperationORM(ctx context.Context, ydbConn *db.YdbConnector, operation types.TakeBackupWithRetryOperation) {
	err := ydbConn.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateOperation(&operation))
	if err != nil {
		log.Panicf("failed to insert operation: %v", err)
	}
	tbwr := ReadTBWROperation(ctx, ydbConn, operation.ID)
	if !reflect.DeepEqual(operation, *tbwr) {
		log.Panicf("operation %v corrupted after read and write\ngot %v", operation, *tbwr)
	}

	err = ydbConn.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateOperation(&operation))
	if err != nil {
		log.Panicf("failed to insert operation: %v", err)
	}
	tbwr = ReadTBWROperation(ctx, ydbConn, operation.ID)
	if !reflect.DeepEqual(operation, *tbwr) {
		log.Panicf("operation %v corrupted after update and read\ngot %v", operation, *tbwr)
	}
	operation.Message = "xxx"
	operation.IncRetries()
	err = ydbConn.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateOperation(&operation))
	if err != nil {
		log.Panicf("failed to insert operation: %v", err)
	}
	tbwr = ReadTBWROperation(ctx, ydbConn, operation.ID)
	if "xxx" != tbwr.Message {
		log.Panicf("operation %v did not change after update", *tbwr)
	}
	if tbwr.Retries != 1 {
		log.Panicf("operation %v did not update retries", *tbwr)
	}
}

func main() {
	metrics.InitializeMockMetricsRegistry()
	ctx := context.Background()
	conn := common.CreateGRPCClient(ydbcpEndpoint)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Panicln("failed to close connection")
		}
	}(conn)
	ydbConn, err := db.NewYdbConnector(
		ctx,
		config.YDBConnectionConfig{
			ConnectionString:   connectionString,
			Insecure:           true,
			Discovery:          false,
			DialTimeoutSeconds: 10,
		},
	)
	if err != nil {
		log.Panicf("failed to create ydb connector: %v", err)
	}
	for _, op := range OperationsToInsert() {
		TestTBWROperationORM(ctx, ydbConn, op)
	}
}
