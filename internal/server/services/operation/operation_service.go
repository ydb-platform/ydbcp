package operation

import (
	"context"

	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/server"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, "ListOperations", zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("ContainerID", request.ContainerId))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	queryFilters := make([]queries.QueryFilter, 0)
	//TODO: forbid empty containerId
	if request.ContainerId != "" {
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
			queries.WithQueryFilters(queryFilters...),
		),
	)
	if err != nil {
		xlog.Error(ctx, "error getting operations", zap.Error(err))
		return nil, status.Error(codes.Internal, "error getting operations")
	}
	pbOperations := make([]*pb.Operation, 0, len(operations))
	for _, operation := range operations {
		pbOperations = append(pbOperations, operation.Proto())
	}
	xlog.Debug(ctx, "success list operations")
	return &pb.ListOperationsResponse{Operations: pbOperations}, nil
}

func (s *OperationService) CancelOperation(
	ctx context.Context,
	request *pb.CancelOperationRequest,
) (*pb.Operation, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, "CancelOperation", zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("OperationID", request.OperationId))

	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(request.GetOperationId())},
				},
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting operation", zap.Error(err))
		return nil, status.Error(codes.Internal, "error getting operation")
	}

	if len(operations) == 0 {
		xlog.Error(ctx, "operation not found")
		return nil, status.Error(codes.NotFound, "operation not found")
	}

	operation := operations[0]
	var permission string

	ctx = xlog.With(
		ctx,
		zap.String("OperationType", operation.GetType().String()),
		zap.String("ContainerID", operation.GetContainerID()),
		zap.String("OperationState", operation.GetState().String()),
	)
	if operation.GetType() == types.OperationTypeTB {
		permission = auth.PermissionBackupCreate
	} else if operation.GetType() == types.OperationTypeRB {
		permission = auth.PermissionBackupRestore
	} else {
		xlog.Error(ctx, "unknown operation type")
		return nil, status.Errorf(codes.Internal, "unknown operation type: %s", operation.GetType().String())
	}

	subject, err := auth.CheckAuth(ctx, s.auth, permission, operation.GetContainerID(), "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if operation.GetState() != types.OperationStatePending {
		xlog.Error(ctx, "can't cancel operation with state != pending")
		return operation.Proto(), nil
	}

	operation.SetState(types.OperationStateStartCancelling)
	operation.SetMessage("Operation was cancelled via OperationService")

	err = s.driver.UpdateOperation(ctx, operation)
	if err != nil {
		xlog.Error(ctx, "error updating operation", zap.Error(err))
		return nil, status.Error(codes.Internal, "error updating operation")
	}

	xlog.Debug(
		ctx, "CancelOperation was started",
		zap.String("operation", types.OperationToString(operation)),
	)
	return operation.Proto(), nil
}

func (s *OperationService) GetOperation(ctx context.Context, request *pb.GetOperationRequest) (*pb.Operation, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, "GetOperation", zap.String("request", request.String()))
	operationID, err := types.ParseObjectID(request.GetId())
	if err != nil {
		xlog.Error(ctx, "failed to parse OperationID", zap.String("OperationID", request.GetId()), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to parse ObjectID")
	}
	ctx = xlog.With(ctx, zap.String("OperationID", operationID))

	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(operationID)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select operations", zap.Error(err))
		return nil, status.Error(codes.Internal, "can't select operations")
	}

	if len(operations) == 0 {
		xlog.Error(ctx, "operation not found")
		return nil, status.Error(codes.NotFound, "operation not found") // TODO: permission denied?
	}
	operation := operations[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", operation.GetContainerID()))
	// TODO: Need to check access to operation resource by operationID
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, operation.GetContainerID(), "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

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
