package backup_schedule

import (
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"strconv"
	"time"
	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/client"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ListSchedulesQuery = fmt.Sprintf(
		`$last_successful_backup_id = SELECT schedule_id, MAX(b.completed_at) AS recovery_point, MAX_BY(b.id, b.completed_at) AS last_successful_backup_id FROM Backups AS b WHERE b.status = '%s' GROUP BY schedule_id;
$last_backup_id = SELECT schedule_id AS schedule_id_2, MAX_BY(b.id, b.completed_at) AS last_backup_id FROM Backups AS b GROUP BY schedule_id;

SELECT * FROM BackupSchedules AS schedules 
LEFT JOIN $last_successful_backup_id AS b1 ON schedules.id = b1.schedule_id
LEFT JOIN $last_backup_id AS b2 ON schedules.id = b2.schedule_id_2
`, types.BackupStateAvailable,
	)
	GetScheduleQuery = fmt.Sprintf(
		`$rpo_info = SELECT 
    <|
        recovery_point: MAX(b.completed_at),
        last_successful_backup_id: MAX_BY(b.id, b.completed_at)
    |> FROM Backups AS b WHERE b.status = '%s' AND b.schedule_id = $schedule_id;

$last_backup_id = SELECT MAX_BY(b.id, b.completed_at) AS last_backup_id FROM Backups AS b WHERE b.schedule_id = $schedule_id;

SELECT s.*, $last_backup_id AS last_backup_id, $rpo_info.recovery_point AS recovery_point, $rpo_info.last_successful_backup_id AS last_successful_backup_id FROM BackupSchedules AS s WHERE s.id = $schedule_id
`, types.BackupStateAvailable,
	)
)

type BackupScheduleService struct {
	pb.UnimplementedBackupScheduleServiceServer
	driver     db.DBConnector
	clientConn client.ClientConnector
	auth       ap.AuthProvider
	clock      clockwork.Clock
}

func (s *BackupScheduleService) CheckClientDbAccess(
	ctx context.Context, clientConnectionParams types.YdbConnectionParams) error {
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	ctx = xlog.With(ctx, zap.String("ClientDSN", dsn))
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	clientDriver, err := s.clientConn.Open(connCtx, dsn)
	if err != nil {
		return status.Errorf(codes.NotFound, err.Error())
	}
	_, err = s.clientConn.ListExportOperations(connCtx, clientDriver)
	if err != nil {
		xlog.Error(ctx, "can't list export operations", zap.Error(err))
		return status.Errorf(codes.PermissionDenied, "user has no access to database %s", dsn)
	}
	return nil
}

func (s *BackupScheduleService) CreateBackupSchedule(
	ctx context.Context, request *pb.CreateBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Info(ctx, "CreateBackupSchedule", zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("ContainerID", request.ContainerId))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, request.ContainerId, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))
	if err = s.CheckClientDbAccess(ctx, types.YdbConnectionParams{
		Endpoint:     request.Endpoint,
		DatabaseName: request.DatabaseName,
	}); err != nil {
		return nil, err
	}
	if request.ScheduleSettings == nil {
		xlog.Error(
			ctx, "no backup schedule settings for CreateBackupSchedule", zap.String("request", request.String()),
		)
		return nil, status.Error(codes.FailedPrecondition, "no backup schedule settings for CreateBackupSchedule")
	}

	if request.ScheduleSettings.RecoveryPointObjective != nil && (request.ScheduleSettings.RecoveryPointObjective.Seconds == 0) {
		return nil, status.Error(codes.FailedPrecondition, "recovery point objective should be greater than 0")
	}
	var scheduleName *string
	if len(request.ScheduleName) > 0 {
		scheduleName = &request.ScheduleName
	}
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
		Name:             scheduleName,
		Status:           types.BackupScheduleStateActive,
		ScheduleSettings: request.ScheduleSettings,
	}
	err = schedule.UpdateNextLaunch(s.clock.Now())
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	err = s.driver.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateBackupSchedule(schedule))
	if err != nil {
		xlog.Error(
			ctx, "can't create backup schedule", zap.String("backup schedule", schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		return nil, status.Error(codes.Internal, "can't create backup schedule")
	}
	xlog.Info(ctx, "backup schedule created", zap.String("BackupScheduleID", schedule.ID))
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) UpdateBackupSchedule(
	ctx context.Context, request *pb.UpdateBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String("BackupScheduleID", scheduleID))

	xlog.Debug(ctx, "UpdateBackupSchedule", zap.Stringer("request", request))

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(GetScheduleQuery),
			queries.WithParameters(
				table.ValueParam("$schedule_id", table_types.StringValueFromString(scheduleID)),
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if len(schedules) == 0 {
		xlog.Error(ctx, "backup schedule not found")
		return nil, status.Error(codes.NotFound, "backup schedule not found")
	}

	schedule := schedules[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", schedule.ContainerID))
	// TODO: Need to check access to backup schedule not by container id?
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, schedule.ContainerID, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))
	if err = s.CheckClientDbAccess(ctx, types.YdbConnectionParams{
		Endpoint:     schedule.DatabaseEndpoint,
		DatabaseName: schedule.DatabaseName,
	}); err != nil {
		return nil, err
	}
	schedule.SourcePaths = request.SourcePaths
	schedule.SourcePathsToExclude = request.SourcePathsToExclude

	if len(request.ScheduleName) > 0 {
		schedule.Name = &request.ScheduleName
	}

	if request.ScheduleSettings != nil {
		if request.ScheduleSettings.SchedulePattern != nil {
			_, err = types.ParseCronExpr(request.ScheduleSettings.SchedulePattern.Crontab)
			if err != nil {
				return nil, status.Error(codes.FailedPrecondition, "failed to parse crontab")
			}
		}

		if request.ScheduleSettings.RecoveryPointObjective != nil && request.ScheduleSettings.RecoveryPointObjective.Seconds == 0 {
			return nil, status.Error(codes.FailedPrecondition, "recovery point objective should be greater than 0")
		}

		schedule.ScheduleSettings = request.ScheduleSettings
		err = schedule.UpdateNextLaunch(s.clock.Now())
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, "failed to update next launch time")
		}
	}

	err = s.driver.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateBackupSchedule(*schedule))
	if err != nil {
		xlog.Error(
			ctx, "can't update backup schedule", zap.String("backup schedule", schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		return nil, status.Error(codes.Internal, "can't update backup schedule")
	}

	xlog.Info(ctx, "UpdateBackupSchedule was completed successfully", zap.Stringer("schedule", schedule))
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) GetBackupSchedule(
	ctx context.Context, request *pb.GetBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String("BackupScheduleID", scheduleID))

	xlog.Debug(ctx, "GetBackupSchedule", zap.Stringer("request", request))

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(GetScheduleQuery),
			queries.WithParameters(
				table.ValueParam("$schedule_id", table_types.StringValueFromString(scheduleID)),
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if len(schedules) == 0 {
		xlog.Error(ctx, "backup schedule not found")
		return nil, status.Error(codes.NotFound, "backup schedule not found") // TODO: Permission denied?
	}

	schedule := schedules[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", schedule.ContainerID))
	// TODO: Need to check access to backup schedule not by container id?
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, schedule.ContainerID, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	xlog.Debug(ctx, "GetBackupSchedule", zap.Stringer("schedule", schedule))
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) ListBackupSchedules(
	ctx context.Context, request *pb.ListBackupSchedulesRequest,
) (*pb.ListBackupSchedulesResponse, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	ctx = xlog.With(ctx, zap.String("ContainerID", request.ContainerId))
	xlog.Debug(ctx, "ListBackupSchedules", zap.String("request", request.String()))

	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

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

	pageSpec, err := queries.NewPageSpec(request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(ListSchedulesQuery),
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
		xlog.Error(ctx, "error getting backup schedules", zap.Error(err))
		return nil, status.Error(codes.Internal, "error getting backup schedules")
	}
	pbSchedules := make([]*pb.BackupSchedule, 0, len(schedules))
	for _, schedule := range schedules {
		pbSchedules = append(pbSchedules, schedule.Proto(s.clock))
	}
	res := &pb.ListBackupSchedulesResponse{Schedules: pbSchedules}
	if uint64(len(pbSchedules)) == pageSpec.Limit {
		res.NextPageToken = strconv.FormatUint(pageSpec.Offset+pageSpec.Limit, 10)
	}
	xlog.Debug(ctx, "ListBackupSchedules success")
	return res, nil
}

func (s *BackupScheduleService) ToggleBackupSchedule(
	ctx context.Context, in *pb.ToggleBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	ctx = xlog.With(ctx, zap.String("BackupScheduleID", in.GetId()))
	xlog.Error(ctx, "ToggleBackupSchedule not implemented")
	//TODO implement me
	return nil, status.Error(codes.Internal, "not implemented")
}

func (s *BackupScheduleService) DeleteBackupSchedule(
	ctx context.Context, request *pb.DeleteBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String("BackupScheduleID", scheduleID))

	xlog.Debug(ctx, "DeleteBackupSchedule", zap.Stringer("request", request))

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(GetScheduleQuery),
			queries.WithParameters(
				table.ValueParam("$schedule_id", table_types.StringValueFromString(scheduleID)),
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if len(schedules) == 0 {
		xlog.Error(ctx, "backup schedule not found")
		return nil, status.Error(codes.NotFound, "backup schedule not found")
	}

	schedule := schedules[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", schedule.ContainerID))
	// TODO: Need to check access to backup schedule not by container id?
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, schedule.ContainerID, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if schedule.Status == types.BackupScheduleStateDeleted {
		xlog.Error(ctx, "backup schedule already deleted")
		return nil, status.Error(codes.FailedPrecondition, "backup schedule already deleted")
	}

	schedule.Status = types.BackupScheduleStateDeleted
	err = s.driver.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateBackupSchedule(*schedule))
	if err != nil {
		xlog.Error(
			ctx, "can't delete backup schedule", zap.String("backup schedule", schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		return nil, status.Error(codes.Internal, "can't delete backup schedule")
	}

	xlog.Info(ctx, "DeleteBackupSchedule was completed successfully", zap.Stringer("schedule", schedule))
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) Register(server server.Server) {
	pb.RegisterBackupScheduleServiceServer(server.GRPCServer(), s)
}

func NewBackupScheduleService(
	driver db.DBConnector,
	clientConn client.ClientConnector,
	auth ap.AuthProvider,
) *BackupScheduleService {
	return &BackupScheduleService{
		driver:     driver,
		clientConn: clientConn,
		auth:       auth,
		clock:      clockwork.NewRealClock(),
	}
}
