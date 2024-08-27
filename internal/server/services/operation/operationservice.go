package operation

import (
	"context"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
		return nil, status.Errorf(codes.Internal, "error getting operations: %v", err)
	}
	pbOperations := make([]*pb.Operation, 0, len(operations))
	for _, operation := range operations {
		pbOperations = append(pbOperations, operation.Proto())
	}
	return &pb.ListOperationsResponse{Operations: pbOperations}, nil
}

func (s *OperationService) CancelOperation(
	ctx context.Context,
	request *pb.CancelOperationRequest,
) (*pb.Operation, error) {
	xlog.Debug(ctx, "CancelOperation", zap.String("request", request.String()))

	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithSelectFields(queries.AllOperationFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(request.GetOperationId())},
				},
			),
		),
	)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting operation: %v", err)
	}

	if len(operations) == 0 {
		return nil, status.Error(codes.NotFound, "operation not found")
	}

	operation := operations[0]
	var permission string

	if operation.GetType() == types.OperationTypeTB {
		permission = auth.PermissionBackupCreate
	} else if operation.GetType() == types.OperationTypeRB {
		permission = auth.PermissionBackupRestore
	} else {
		return nil, status.Errorf(codes.Internal, "unknown operation type: %v", operation.GetType())
	}

	if _, err := auth.CheckAuth(ctx, s.auth, permission, operation.GetContainerID(), ""); err != nil {
		return nil, err
	}

	if operation.GetState() != types.OperationStatePending {
		xlog.Warn(ctx, "can't cancel operation with state != pending",
			zap.String("state", operation.GetState().String()),
		)
		return operation.Proto(), nil
	}

	operation.SetState(types.OperationStateStartCancelling)
	operation.SetMessage("Operation was cancelled via OperationService")

	err = s.driver.UpdateOperation(ctx, operation)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error updating operation: %v", err)
	}

	xlog.Debug(ctx, "CancelOperation was started",
		zap.String("operation", types.OperationToString(operation)),
	)
	return operation.Proto(), nil
}

func (s *OperationService) GetOperation(ctx context.Context, request *pb.GetOperationRequest) (*pb.Operation, error) {
	xlog.Debug(ctx, "GetOperation", zap.String("request", request.String()))
	requestId, err := types.ParseObjectID(request.GetId())
	if err != nil {
		xlog.Error(ctx, "failed to parse ObjectID", zap.String("ObjectID", request.GetId()), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to parse ObjectID: %v", err)
	}
	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithSelectFields(queries.AllOperationFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(requestId)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select operations", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "can't select operations: %v", err)
	}

	if len(operations) == 0 {
		return nil, status.Error(codes.NotFound, "operation not found") // TODO: permission denied?
	}
	// TODO: Need to check access to operation resource by operationID
	if _, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, operations[0].GetContainerID(), ""); err != nil {
		return nil, err
	}

	xlog.Debug(ctx, "GetOperation", zap.String("operation", types.OperationToString(operations[0])))
	return operations[0].Proto(), nil
}

func (s *OperationService) Register(server server.Server) {
	pb.RegisterOperationServiceServer(server.GRPCServer(), s)
}

func NewOperationService(driver db.DBConnector, auth ap.AuthProvider) *OperationService {
	return &OperationService{
		driver: driver,
		auth:   auth,
	}
}
