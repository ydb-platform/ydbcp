package operation

import (
	"context"
	"errors"
	"fmt"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"

	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/server"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

type OperationService struct {
	pb.UnimplementedOperationServiceServer
	driver db.DBConnector
	auth   ap.AuthProvider
}

func (s *OperationService) Register(server server.Server) {
	pb.RegisterOperationServiceServer(server.GRPCServer(), s)
}

func (s *OperationService) ListOperations(
	ctx context.Context,
	request *pb.ListOperationsRequest,
) (*pb.ListOperationsResponse, error) {
	xlog.Debug(ctx, "ListOperations", zap.String("request", request.String()))
	if _, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, ""); err != nil {
		return nil, err
	}

	queryFilters := make([]queries.QueryFilter, 0)
	//TODO: forbid empty containerId
	if request.GetContainerId() != "" {
		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field: "container_id",
				Values: []table_types.Value{
					table_types.StringValueFromString(request.ContainerId),
				},
			},
		)
	}
	if request.GetDatabaseNameMask() != "" {
		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field: "database",
				Values: []table_types.Value{
					table_types.StringValueFromString(request.DatabaseNameMask),
				},
				IsLike: true,
			},
		)
	}

	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithSelectFields(queries.AllOperationFields...),
			queries.WithQueryFilters(queryFilters...),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting operations: %w", err)
	}
	pbOperations := make([]*pb.Operation, 0, len(operations))
	for _, operation := range operations {
		pbOperations = append(pbOperations, operation.Proto())
	}
	return &pb.ListOperationsResponse{
		Operations: pbOperations,
	}, nil
}

func (s *OperationService) CancelOperation(ctx context.Context, request *pb.CancelOperationRequest) (*pb.Operation, error) {
	//TODO implement me
	return nil, errors.New("not implemented")
}

func (s *OperationService) GetOperation(ctx context.Context, request *pb.GetOperationRequest) (*pb.Operation, error) {
	xlog.Debug(ctx, "GetOperation", zap.String("request", request.String()))
	requestId, err := types.ParseObjectId(request.GetId())
	if err != nil {
		return nil, fmt.Errorf("failed to parse uuid %s: %w", request.GetId(), err)
	}
	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithSelectFields(queries.AllOperationFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.UUIDValue(requestId)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select operations", zap.Error(err))
		return nil, err
	}
	if len(operations) == 0 {
		return nil, errors.New("no operation with such Id") // TODO: Permission denied?
	}
	// TODO: Need to check access to operation resource by operationID
	if _, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, operations[0].GetContainerId(), ""); err != nil {
		return nil, err
	}

	xlog.Debug(ctx, "GetOperation", zap.String("operation", types.OperationToString(operations[0])))
	return operations[0].Proto(), nil
}

func NewOperationService(driver db.DBConnector, auth ap.AuthProvider) *OperationService {
	return &OperationService{
		driver: driver,
		auth:   auth,
	}
}
