package backup_schedule

import (
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"google.golang.org/protobuf/types/known/durationpb"
	"strconv"
	"time"
	"ydbcp/internal/auth"
	"ydbcp/internal/backup_operations"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/metrics"
	"ydbcp/internal/server"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/types"
	"ydbcp/internal/util/helpers"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackupScheduleService struct {
	pb.UnimplementedBackupScheduleServiceServer
	driver     db.DBConnector
	clientConn client.ClientConnector
	auth       ap.AuthProvider
	clock      clockwork.Clock
	config     config.Config
}

func (s *BackupScheduleService) IncApiCallsCounter(methodName string, code codes.Code) {
	metrics.GlobalMetricsRegistry.IncApiCallsCounter("BackupScheduleService", methodName, code.String())
}

func (s *BackupScheduleService) CreateBackupSchedule(
	ctx context.Context, request *pb.CreateBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	const methodName string = "CreateBackupSchedule"
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, methodName, zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("ContainerID", request.ContainerId))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, request.ContainerId, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))
	if err = helpers.CheckClientDbAccess(ctx, s.clientConn, types.YdbConnectionParams{
		Endpoint:     request.Endpoint,
		DatabaseName: request.DatabaseName,
	}); err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}

	schedules, err := s.driver.SelectBackupSchedules(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("BackupSchedules"),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field: "container_id",
					Values: []table_types.Value{
						table_types.StringValueFromString(request.ContainerId),
					},
				},
				queries.QueryFilter{
					Field: "database",
					Values: []table_types.Value{
						table_types.StringValueFromString(request.DatabaseName),
					},
				},
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting backup schedules", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedules")
	}

	if len(schedules)+1 > s.config.SchedulesLimitPerDB {
		xlog.Error(ctx, "can't create backup schedule, limit exceeded for database",
			zap.String("database", request.DatabaseName),
			zap.String("container", request.ContainerId),
			zap.Int("limit", s.config.SchedulesLimitPerDB),
		)
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"can't create backup schedule, limit exceeded for database: %s, container: %s, limit: %d",
			request.DatabaseName,
			request.ContainerId,
			s.config.SchedulesLimitPerDB,
		)
	}

	if request.ScheduleSettings == nil {
		xlog.Error(
			ctx, "no backup schedule settings for CreateBackupSchedule", zap.String("request", request.String()),
		)
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, "no backup schedule settings for CreateBackupSchedule")
	}

	if request.ScheduleSettings.RecoveryPointObjective != nil && (request.ScheduleSettings.RecoveryPointObjective.Seconds == 0) {
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
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

	if schedule.ScheduleSettings.RecoveryPointObjective == nil {
		duration, err := schedule.GetCronDuration()
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get cron duration: %v", err))
		}
		schedule.ScheduleSettings.RecoveryPointObjective = durationpb.New(duration + time.Hour)
	}

	err = backup_operations.OpenConnAndValidateSourcePaths(ctx, backup_operations.FromBackupSchedule(&schedule), s.clientConn)
	if err != nil {
		return nil, err
	}

	err = schedule.UpdateNextLaunch(s.clock.Now())
	if err != nil {
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	err = s.driver.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateBackupSchedule(schedule))
	if err != nil {
		xlog.Error(
			ctx, "can't create backup schedule", zap.String("backup schedule", schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't create backup schedule")
	}
	xlog.Debug(ctx, methodName, zap.Stringer("schedule", &schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) UpdateBackupSchedule(
	ctx context.Context, request *pb.UpdateBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	const methodName string = "UpdateBackupSchedule"
	ctx = grpcinfo.WithGRPCInfo(ctx)

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String("BackupScheduleID", scheduleID))

	xlog.Debug(ctx, methodName, zap.Stringer("request", request))

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(queries.GetScheduleQuery),
			queries.WithParameters(
				table.ValueParam("$schedule_id", table_types.StringValueFromString(scheduleID)),
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if len(schedules) == 0 {
		xlog.Error(ctx, "backup schedule not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup schedule not found")
	}

	schedule := schedules[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", schedule.ContainerID))
	// TODO: Need to check access to backup schedule not by container id?
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, schedule.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))
	if err = helpers.CheckClientDbAccess(ctx, s.clientConn, types.YdbConnectionParams{
		Endpoint:     schedule.DatabaseEndpoint,
		DatabaseName: schedule.DatabaseName,
	}); err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}

	if schedule.Status == types.BackupScheduleStateDeleted {
		xlog.Error(ctx, "backup schedule was deleted")
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, "backup schedule was deleted")
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
				s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
				return nil, status.Error(codes.FailedPrecondition, "failed to parse crontab")
			}
		}

		if request.ScheduleSettings.RecoveryPointObjective != nil && request.ScheduleSettings.RecoveryPointObjective.Seconds == 0 {
			s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
			return nil, status.Error(codes.FailedPrecondition, "recovery point objective should be greater than 0")
		}

		schedule.ScheduleSettings = request.ScheduleSettings
		err = schedule.UpdateNextLaunch(s.clock.Now())
		if err != nil {
			s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
			return nil, status.Error(codes.FailedPrecondition, "failed to update next launch time")
		}
	}

	err = backup_operations.OpenConnAndValidateSourcePaths(ctx, backup_operations.FromBackupSchedule(schedule), s.clientConn)
	if err != nil {
		return nil, err
	}

	err = s.driver.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateBackupSchedule(*schedule))
	if err != nil {
		xlog.Error(
			ctx, "can't update backup schedule", zap.String("backup schedule", schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't update backup schedule")
	}

	xlog.Debug(ctx, methodName, zap.Stringer("schedule", schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) GetBackupSchedule(
	ctx context.Context, request *pb.GetBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	const methodName string = "GetBackupSchedule"
	ctx = grpcinfo.WithGRPCInfo(ctx)

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String("BackupScheduleID", scheduleID))

	xlog.Debug(ctx, methodName, zap.Stringer("request", request))

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(queries.GetScheduleQuery),
			queries.WithParameters(
				table.ValueParam("$schedule_id", table_types.StringValueFromString(scheduleID)),
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if len(schedules) == 0 {
		xlog.Error(ctx, "backup schedule not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup schedule not found") // TODO: Permission denied?
	}

	schedule := schedules[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", schedule.ContainerID))
	// TODO: Need to check access to backup schedule not by container id?
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, schedule.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	xlog.Debug(ctx, methodName, zap.Stringer("schedule", schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) ListBackupSchedules(
	ctx context.Context, request *pb.ListBackupSchedulesRequest,
) (*pb.ListBackupSchedulesResponse, error) {
	const methodName string = "ListBackupSchedules"
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, methodName, zap.String("request", request.String()))

	queryFilters := make([]queries.QueryFilter, 0)
	checkEveryCID := false
	subjectLabel := true

	if request.GetContainerId() != "" {
		ctx = xlog.With(ctx, zap.String("ContainerID", request.GetContainerId()))
		subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, "")
		if err != nil {
			s.IncApiCallsCounter(methodName, status.Code(err))
			return nil, err
		}
		ctx = xlog.With(ctx, zap.String("SubjectID", subject))

		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field: "container_id",
				Values: []table_types.Value{
					table_types.StringValueFromString(request.ContainerId),
				},
			},
		)
	} else {
		checkEveryCID = true
		subjectLabel = false
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
	if len(request.GetDisplayStatus()) > 0 {
		var statusValues []table_types.Value
		for _, statusValue := range request.GetDisplayStatus() {
			statusValues = append(statusValues, table_types.StringValueFromString(statusValue.String()))
		}

		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field:  "status",
				Values: statusValues,
			},
		)
	}

	pageSpec, err := queries.NewPageSpec(request.GetPageSize(), request.GetPageToken())
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(queries.ListSchedulesQuery),
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedules")
	}
	pbSchedules := make([]*pb.BackupSchedule, 0, len(schedules))
	checkedCIDs := make(map[string]bool)
	for _, schedule := range schedules {
		if checkEveryCID && !checkedCIDs[schedule.ContainerID] {
			checkedCIDs[schedule.ContainerID] = true
			subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, schedule.ContainerID, "")
			if !subjectLabel {
				ctx = xlog.With(ctx, zap.String("SubjectID", subject))
				subjectLabel = true
			}
			if err != nil {
				continue
			}
		}
		pbSchedules = append(pbSchedules, schedule.Proto(s.clock))
	}
	res := &pb.ListBackupSchedulesResponse{Schedules: pbSchedules}
	if uint64(len(pbSchedules)) == pageSpec.Limit {
		res.NextPageToken = strconv.FormatUint(pageSpec.Offset+pageSpec.Limit, 10)
	}
	s.IncApiCallsCounter(methodName, codes.OK)
	return res, nil
}

func (s *BackupScheduleService) ToggleBackupSchedule(
	ctx context.Context, request *pb.ToggleBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	const methodName string = "ToggleBackupSchedule"
	ctx = grpcinfo.WithGRPCInfo(ctx)

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String("BackupScheduleID", scheduleID))

	xlog.Debug(ctx, methodName, zap.Stringer("request", request))

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(queries.GetScheduleQuery),
			queries.WithParameters(
				table.ValueParam("$schedule_id", table_types.StringValueFromString(scheduleID)),
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if len(schedules) == 0 {
		xlog.Error(ctx, "backup schedule not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup schedule not found")
	}

	schedule := schedules[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", schedule.ContainerID))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, schedule.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))
	if err = helpers.CheckClientDbAccess(ctx, s.clientConn, types.YdbConnectionParams{
		Endpoint:     schedule.DatabaseEndpoint,
		DatabaseName: schedule.DatabaseName,
	}); err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}

	if schedule.Status == types.BackupScheduleStateDeleted {
		xlog.Error(ctx, "backup schedule was deleted")
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, "backup schedule was deleted")
	}

	if request.GetActiveState() {
		schedule.Status = types.BackupScheduleStateActive
	} else {
		schedule.Status = types.BackupScheduleStateInactive
	}

	if schedule.ScheduleSettings != nil {
		err = schedule.UpdateNextLaunch(s.clock.Now())
		if err != nil {
			s.IncApiCallsCounter(methodName, codes.Internal)
			return nil, status.Error(codes.Internal, "failed to update next launch time")
		}
	}

	err = s.driver.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateBackupSchedule(*schedule))
	if err != nil {
		xlog.Error(
			ctx, "can't update backup schedule", zap.String("backup schedule", schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't update backup schedule")
	}

	if schedule.Status == types.BackupScheduleStateInactive {
		metrics.GlobalMetricsRegistry.ResetScheduleCounters(schedule)
	}

	xlog.Debug(ctx, methodName, zap.Stringer("schedule", schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) DeleteBackupSchedule(
	ctx context.Context, request *pb.DeleteBackupScheduleRequest,
) (*pb.BackupSchedule, error) {
	const methodName string = "DeleteBackupSchedule"
	ctx = grpcinfo.WithGRPCInfo(ctx)

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String("BackupScheduleID", scheduleID))

	xlog.Debug(ctx, methodName, zap.Stringer("request", request))

	schedules, err := s.driver.SelectBackupSchedulesWithRPOInfo(
		ctx, queries.NewReadTableQuery(
			queries.WithRawQuery(queries.GetScheduleQuery),
			queries.WithParameters(
				table.ValueParam("$schedule_id", table_types.StringValueFromString(scheduleID)),
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if len(schedules) == 0 {
		xlog.Error(ctx, "backup schedule not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup schedule not found")
	}

	schedule := schedules[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", schedule.ContainerID))
	// TODO: Need to check access to backup schedule not by container id?
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, schedule.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if schedule.Status == types.BackupScheduleStateDeleted {
		xlog.Error(ctx, "backup schedule already deleted")
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, "backup schedule already deleted")
	}

	schedule.Status = types.BackupScheduleStateDeleted
	err = s.driver.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateBackupSchedule(*schedule))
	if err != nil {
		xlog.Error(
			ctx, "can't delete backup schedule", zap.String("backup schedule", schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't delete backup schedule")
	}

	metrics.GlobalMetricsRegistry.ResetScheduleCounters(schedule)

	xlog.Debug(ctx, methodName, zap.Stringer("schedule", schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) Register(server server.Server) {
	pb.RegisterBackupScheduleServiceServer(server.GRPCServer(), s)
}

func NewBackupScheduleService(
	driver db.DBConnector,
	clientConn client.ClientConnector,
	auth ap.AuthProvider,
	config config.Config,
) *BackupScheduleService {
	return &BackupScheduleService{
		driver:     driver,
		clientConn: clientConn,
		auth:       auth,
		clock:      clockwork.NewRealClock(),
		config:     config,
	}
}
