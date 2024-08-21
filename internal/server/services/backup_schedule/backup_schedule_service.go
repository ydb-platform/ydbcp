package backup_schedule

import (
	"context"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/server"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

type BackupScheduleService struct {
	pb.UnimplementedBackupScheduleServiceServer
	driver db.DBConnector
	auth   ap.AuthProvider
}

func (s *BackupScheduleService) CreateBackupSchedule(
	ctx context.Context, request *pb.CreateBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	xlog.Info(ctx, "CreateBackupSchedule", zap.String("request", request.String()))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, request.ContainerId, "")
	if err != nil {
		return nil, err
	}
	if request.ScheduleSettings == nil {
		xlog.Error(
			ctx, "no backup schedule settings for CreateBackupSchedule", zap.String("request", request.String()),
		)
		return nil, status.Errorf(codes.FailedPrecondition, "no backup schedule settings for CreateBackupSchedule")

	}
	xlog.Debug(ctx, "CreateBackupSchedule", zap.String("subject", subject))
	now := time.Now()
	schedule := types.BackupSchedule{
		ID:                   types.GenerateObjectID(),
		ContainerID:          request.ContainerId,
		DatabaseName:         request.DatabaseName,
		DatabaseEndpoint:     request.Endpoint,
		SourcePaths:          request.SourcePaths,
		SourcePathsToExclude: request.SourcePathsToExclude,
		Audit: &pb.AuditInfo{
			Creator:   subject,
			CreatedAt: timestamppb.Now(),
		},
		Name:             request.ScheduleName,
		Active:           true,
		ScheduleSettings: request.ScheduleSettings,
		NextLaunch:       &now,
	}

	err = s.driver.ExecuteUpsert(ctx, queries.NewWriteTableQuery(ctx).WithCreateBackupSchedule(schedule))
	if err != nil {
		xlog.Error(
			ctx, "can't create backup schedule", zap.String("backup schedule", schedule.Proto().String()),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "can't create backup schedule: %v", err)
	}
	return schedule.Proto(), nil
}

func (s *BackupScheduleService) UpdateBackupSchedule(
	ctx context.Context, in *pb.UpdateBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	//TODO implement me
	return nil, status.Error(codes.Internal, "not implemented")
}

func (s *BackupScheduleService) GetBackupSchedule(
	ctx context.Context, in *pb.GetBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	//TODO implement me
	return nil, status.Error(codes.Internal, "not implemented")
}

func (s *BackupScheduleService) ListBackupSchedules(
	ctx context.Context, request *pb.ListBackupSchedulesRequest,
) (*pb.ListBackupSchedulesResponse, error) {
	xlog.Debug(ctx, "ListBackupSchedules", zap.String("request", request.String()))
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

	schedules, err := s.driver.SelectBackupSchedules(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("BackupSchedules"),
			queries.WithSelectFields(queries.AllBackupScheduleFields...),
			queries.WithQueryFilters(queryFilters...),
		),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting backup schedules: %v", err)
	}
	pbSchedules := make([]*pb.BackupSchedule, 0, len(schedules))
	for _, schedule := range schedules {
		pbSchedules = append(pbSchedules, schedule.Proto())
	}
	return &pb.ListBackupSchedulesResponse{Schedules: pbSchedules}, nil
}

func (s *BackupScheduleService) ToggleBackupSchedule(
	ctx context.Context, in *pb.ToggleBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	//TODO implement me
	return nil, status.Error(codes.Internal, "not implemented")
}

func (s *BackupScheduleService) Register(server server.Server) {
	pb.RegisterBackupScheduleServiceServer(server.GRPCServer(), s)
}

func NewBackupScheduleService(
	driver db.DBConnector,
	auth ap.AuthProvider,
) *BackupScheduleService {
	return &BackupScheduleService{
		driver: driver,
		auth:   auth,
	}
}
