package operation

import (
	"context"
	"strconv"
	"ydbcp/internal/audit"
	"ydbcp/internal/metrics"

	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/server"
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

func (s *OperationService) IncApiCallsCounter(methodName string, code codes.Code) {
	metrics.GlobalMetricsRegistry.IncApiCallsCounter("OperationService", methodName, code.String())
}

func (s *OperationService) ListOperations(
	ctx context.Context,
	request *pb.ListOperationsRequest,
) (_ *pb.ListOperationsResponse, responseErr error) {
	const methodName string = "ListOperations"
	xlog.Debug(ctx, methodName, zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("ContainerID", request.ContainerId))
	audit.SetAuditFieldsForRequest(
		ctx, &audit.AuditFields{ContainerID: request.ContainerId, Database: "{none}"},
	)
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
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
	if len(request.OperationTypes) > 0 {
		typeValues := make([]table_types.Value, len(request.OperationTypes))
		for i, opType := range request.OperationTypes {
			typeValues[i] = table_types.StringValueFromString(opType)
		}
		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field:  "type",
				Values: typeValues,
			},
		)
	}

	pageSpec, err := queries.NewPageSpec(request.GetPageSize(), request.GetPageToken())
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}

	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithQueryFilters(queryFilters...),
			queries.WithOrderBy(
				queries.OrderSpec{
					Field: "created_at",
					Desc:  true,
				},
			),
			queries.WithPageSpec(*pageSpec),
		),
	)
	if err != nil {
		xlog.Error(ctx, "error getting operations", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting operations")
	}
	pbOperations := make([]*pb.Operation, 0, len(operations))
	for _, operation := range operations {
		pbOperations = append(pbOperations, operation.Proto())
	}
	res := &pb.ListOperationsResponse{Operations: pbOperations}
	if uint64(len(pbOperations)) == pageSpec.Limit {
		res.NextPageToken = strconv.FormatUint(pageSpec.Offset+pageSpec.Limit, 10)
	}
	xlog.Debug(ctx, methodName, zap.Stringer("response", res))
	s.IncApiCallsCounter(methodName, codes.OK)
	return res, nil
}

func (s *OperationService) CancelOperation(
	ctx context.Context,
	request *pb.CancelOperationRequest,
) (_ *pb.Operation, responseErr error) {
	const methodName string = "CancelOperation"
	xlog.Debug(ctx, methodName, zap.String("request", request.String()))
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting operation")
	}

	if len(operations) == 0 {
		xlog.Error(ctx, "operation not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
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
	} else if operation.GetType() == types.OperationTypeDB {
		xlog.Error(ctx, "can't cancel DeleteBackup operation")
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Errorf(
			codes.FailedPrecondition, "can't cancel DeleteBackup operation: %s", types.OperationToString(operation),
		)
	} else {
		xlog.Error(ctx, "unknown operation type")
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Errorf(codes.Internal, "unknown operation type: %s", operation.GetType().String())
	}

	audit.SetAuditFieldsForRequest(
		ctx, &audit.AuditFields{ContainerID: operation.GetContainerID(), Database: operation.GetDatabaseName()},
	)
	subject, err := auth.CheckAuth(ctx, s.auth, permission, operation.GetContainerID(), "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if operation.GetState() != types.OperationStatePending && operation.GetState() != types.OperationStateRunning {
		xlog.Error(
			ctx, "can't cancel operation with state", zap.String("OperationState", operation.GetState().String()),
		)
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Errorf(
			codes.FailedPrecondition, "can't cancel operation with state: %s", operation.GetState().String(),
		)
	}

	operation.SetState(types.OperationStateStartCancelling)
	operation.SetMessage("Operation was cancelled via OperationService")

	err = s.driver.UpdateOperation(ctx, operation)
	if err != nil {
		xlog.Error(ctx, "error updating operation", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error updating operation")
	}

	xlog.Debug(
		ctx, methodName,
		zap.String("operation", types.OperationToString(operation)),
	)
	s.IncApiCallsCounter(methodName, codes.OK)
	return operation.Proto(), nil
}

func (s *OperationService) GetOperation(ctx context.Context, request *pb.GetOperationRequest) (
	_ *pb.Operation, responseErr error,
) {
	const methodName string = "GetOperation"
	xlog.Debug(ctx, methodName, zap.String("request", request.String()))
	operationID, err := types.ParseObjectID(request.GetId())
	if err != nil {
		xlog.Error(ctx, "failed to parse OperationID", zap.String("OperationID", request.GetId()), zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't select operations")
	}

	if len(operations) == 0 {
		xlog.Error(ctx, "operation not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "operation not found") // TODO: permission denied?
	}
	operation := operations[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", operation.GetContainerID()))
	// TODO: Need to check access to operation resource by operationID
	audit.SetAuditFieldsForRequest(
		ctx, &audit.AuditFields{ContainerID: operation.GetContainerID(), Database: operation.GetDatabaseName()},
	)

	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, operation.GetContainerID(), "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	xlog.Debug(ctx, methodName, zap.String("operation", types.OperationToString(operations[0])))
	s.IncApiCallsCounter(methodName, codes.OK)
	return operations[0].Proto(), nil
}

func (s *OperationService) Register(server server.Server) {
	pb.RegisterOperationServiceServer(server.GRPCServer(), s)
}

func NewOperationService(
	driver db.DBConnector,
	auth ap.AuthProvider,
) *OperationService {
	return &OperationService{
		driver: driver,
		auth:   auth,
	}
}
