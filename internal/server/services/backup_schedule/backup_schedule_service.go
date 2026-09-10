package backup_schedule

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	pb "github.com/ydb-platform/ydbcp/pkg/proto/ydbcp/v1alpha1"

	"ydbcp/internal/audit"
	"ydbcp/internal/auth"
	"ydbcp/internal/backup_operations"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	dbconnector "ydbcp/internal/connectors/db"
	"ydbcp/internal/metrics"
	"ydbcp/internal/server"
	"ydbcp/internal/server/services/listoptions"
	"ydbcp/internal/types"
	"ydbcp/internal/util/helpers"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackupScheduleService struct {
	pb.UnimplementedBackupScheduleServiceServer
	driver     dbconnector.DBConnector
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
) (_ *pb.BackupSchedule, responseErr error) {
	const methodName string = "CreateBackupSchedule"
	xlog.Debug(ctx, methodName, zap.String(log_keys.Request, request.String()))
	ctx = xlog.With(ctx, zap.String(log_keys.ContainerID, request.ContainerId))
	audit.SetAuditFieldsForRequest(
		ctx, &audit.AuditFields{ContainerID: request.ContainerId, Database: request.DatabaseName},
	)
	subject, err := auth.CheckCreateScheduleAuth(ctx, s.auth, request.ContainerId, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String(log_keys.Subject, subject))
	if err = helpers.CheckClientDbAccess(
		ctx, s.clientConn, types.YdbConnectionParams{
			Endpoint:     request.Endpoint,
			DatabaseName: request.DatabaseName,
		},
	); err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}

	schedules, err := s.driver.ListSchedules(ctx, dbconnector.ScheduleFilter{ContainerID: request.ContainerId, DatabaseName: request.DatabaseName})

	if err != nil {
		xlog.Error(ctx, "error getting backup schedules", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedules")
	}

	if len(schedules)+1 > s.config.Quota.SchedulesPerDB {
		xlog.Error(
			ctx, "can't create backup schedule, limit exceeded for database",
			zap.String(log_keys.Database, request.DatabaseName),
			zap.String(log_keys.ContainerID, request.ContainerId),
			zap.Int(log_keys.Limit, s.config.Quota.SchedulesPerDB),
		)
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"can't create backup schedule, limit exceeded for database: %s, container: %s, limit: %d",
			request.DatabaseName,
			request.ContainerId,
			s.config.Quota.SchedulesPerDB,
		)
	}

	if request.ScheduleSettings == nil {
		xlog.Error(
			ctx, "no backup schedule settings for CreateBackupSchedule", zap.String(log_keys.Request, request.String()),
		)
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, "no backup schedule settings for CreateBackupSchedule")
	}

	if request.ScheduleSettings.EncryptionSettings != nil {
		if !s.config.FeatureFlags.EnableBackupsEncryption {
			s.IncApiCallsCounter(methodName, codes.Unimplemented)
			return nil, status.Error(codes.Unimplemented, "backup encryption is not supported yet")
		}

		if request.ScheduleSettings.EncryptionSettings.GetKeyEncryptionKey() == nil {
			s.IncApiCallsCounter(methodName, codes.InvalidArgument)
			return nil, status.Error(codes.InvalidArgument, "encryption key is required")
		}
	}

	if request.ScheduleSettings.RecoveryPointObjective != nil && (request.ScheduleSettings.RecoveryPointObjective.Seconds == 0) {
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, "recovery point objective should be greater than 0")
	}

	if len(request.RootPath) > 0 && !s.config.FeatureFlags.EnableNewPathsFormat {
		s.IncApiCallsCounter(methodName, codes.Unimplemented)
		return nil, status.Error(codes.Unimplemented, "backup root path is not supported yet")
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
		RootPath:             request.RootPath,
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
	ctx = schedule.SetLogFields(ctx)
	if schedule.ScheduleSettings.RecoveryPointObjective == nil {
		duration, err := schedule.GetCronDuration()
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get cron duration: %v", err))
		}
		schedule.ScheduleSettings.RecoveryPointObjective = durationpb.New(duration + time.Hour)
	}

	err = backup_operations.OpenConnAndValidateSourcePaths(
		ctx, backup_operations.FromBackupSchedule(&schedule), s.clientConn, s.config.FeatureFlags,
	)
	if err != nil {
		return nil, err
	}

	err = schedule.UpdateNextLaunch(s.clock.Now())
	if err != nil {
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	err = s.driver.Apply(ctx, dbconnector.Changes{CreateSchedules: []types.BackupSchedule{schedule}})
	if err != nil {
		xlog.Error(
			ctx, "can't create backup schedule", zap.String(log_keys.BackupSchedule, schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't create backup schedule")
	}
	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.BackupSchedule, &schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) UpdateBackupSchedule(
	ctx context.Context, request *pb.UpdateBackupScheduleRequest,
) (_ *pb.BackupSchedule, responseErr error) {
	const methodName string = "UpdateBackupSchedule"

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String(log_keys.ScheduleID, scheduleID))

	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.Request, request))

	schedule, err := s.driver.GetScheduleWithBackupInfo(ctx, scheduleID)

	if err != nil && !errors.Is(err, dbconnector.ErrNotFound) {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if errors.Is(err, dbconnector.ErrNotFound) {
		xlog.Error(ctx, "backup schedule not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup schedule not found")
	}

	ctx = schedule.SetLogFields(ctx)
	// TODO: Need to check access to backup schedule not by container id?
	audit.SetAuditFieldsForRequest(
		ctx, &audit.AuditFields{ContainerID: schedule.ContainerID, Database: schedule.DatabaseName},
	)
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, schedule.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String(log_keys.Subject, subject))
	if err = helpers.CheckClientDbAccess(
		ctx, s.clientConn, types.YdbConnectionParams{
			Endpoint:     schedule.DatabaseEndpoint,
			DatabaseName: schedule.DatabaseName,
		},
	); err != nil {
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
			schedule.ScheduleSettings.SchedulePattern = request.ScheduleSettings.SchedulePattern
		}

		if request.ScheduleSettings.RecoveryPointObjective != nil && request.ScheduleSettings.RecoveryPointObjective.Seconds == 0 {
			s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
			return nil, status.Error(codes.FailedPrecondition, "recovery point objective should be greater than 0")
		}
		if request.ScheduleSettings.RecoveryPointObjective != nil {
			schedule.ScheduleSettings.RecoveryPointObjective = request.ScheduleSettings.RecoveryPointObjective
		}
		if request.ScheduleSettings.Ttl != nil {
			schedule.ScheduleSettings.Ttl = request.ScheduleSettings.Ttl
		}

		err = schedule.UpdateNextLaunch(s.clock.Now())
		if err != nil {
			s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
			return nil, status.Error(codes.FailedPrecondition, "failed to update next launch time")
		}
	}

	err = backup_operations.OpenConnAndValidateSourcePaths(
		ctx, backup_operations.FromBackupSchedule(schedule), s.clientConn, s.config.FeatureFlags,
	)
	if err != nil {
		return nil, err
	}

	err = s.driver.Apply(ctx, dbconnector.Changes{UpdateSchedules: []types.BackupSchedule{*schedule}})
	if err != nil {
		xlog.Error(
			ctx, "can't update backup schedule", zap.String(log_keys.BackupSchedule, schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't update backup schedule")
	}

	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.BackupSchedule, schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) GetBackupSchedule(
	ctx context.Context, request *pb.GetBackupScheduleRequest,
) (_ *pb.BackupSchedule, responseErr error) {
	const methodName string = "GetBackupSchedule"
	ctx = xlog.With(ctx, zap.String(log_keys.GRPCCall, pb.BackupScheduleService_GetBackupSchedule_FullMethodName))

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String(log_keys.ScheduleID, scheduleID))

	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.Request, request))

	schedule, err := s.driver.GetScheduleWithBackupInfo(ctx, scheduleID)

	if err != nil && !errors.Is(err, dbconnector.ErrNotFound) {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if errors.Is(err, dbconnector.ErrNotFound) {
		xlog.Error(ctx, "backup schedule not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup schedule not found") // TODO: Permission denied?
	}

	ctx = schedule.SetLogFields(ctx)
	audit.SetAuditFieldsForRequest(
		ctx, &audit.AuditFields{ContainerID: schedule.ContainerID, Database: schedule.DatabaseName},
	)
	// TODO: Need to check access to backup schedule not by container id?
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, schedule.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String(log_keys.Subject, subject))

	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.BackupSchedule, schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) ListBackupSchedules(
	ctx context.Context, request *pb.ListBackupSchedulesRequest,
) (_ *pb.ListBackupSchedulesResponse, responseErr error) {
	const methodName string = "ListBackupSchedules"
	xlog.Debug(ctx, methodName, zap.String(log_keys.Request, request.String()))

	filter := dbconnector.ScheduleFilter{
		ContainerID:      request.GetContainerId(),
		DatabaseNameMask: request.GetDatabaseNameMask(),
	}
	checkEveryCID := false
	subjectLabel := true
	if request.GetContainerId() != "" {
		ctx = xlog.With(ctx, zap.String(log_keys.ContainerID, request.ContainerId))
		audit.SetAuditFieldsForRequest(ctx, &audit.AuditFields{ContainerID: request.ContainerId, Database: "{none}"})
		subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, "")
		if err != nil {
			s.IncApiCallsCounter(methodName, status.Code(err))
			return nil, err
		}
		ctx = xlog.With(ctx, zap.String(log_keys.Subject, subject))
	} else {
		checkEveryCID = true
		subjectLabel = false
	}
	for _, value := range request.GetDisplayStatus() {
		filter.Statuses = append(filter.Statuses, value.String())
	}
	pageSpec, err := listoptions.Page(request.GetPageSize(), request.GetPageToken())
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	filter.Page = pageSpec
	schedules, err := s.driver.ListSchedulesWithBackupInfo(ctx, filter)
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
				ctx = xlog.With(ctx, zap.String(log_keys.Subject, subject))
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
) (_ *pb.BackupSchedule, responseErr error) {
	const methodName string = "ToggleBackupSchedule"

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String(log_keys.ScheduleID, scheduleID))

	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.Request, request))

	schedule, err := s.driver.GetScheduleWithBackupInfo(ctx, scheduleID)

	if err != nil && !errors.Is(err, dbconnector.ErrNotFound) {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if errors.Is(err, dbconnector.ErrNotFound) {
		xlog.Error(ctx, "backup schedule not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup schedule not found")
	}

	ctx = schedule.SetLogFields(ctx)
	audit.SetAuditFieldsForRequest(
		ctx, &audit.AuditFields{ContainerID: schedule.ContainerID, Database: schedule.DatabaseName},
	)
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, schedule.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String(log_keys.Subject, subject))
	if err = helpers.CheckClientDbAccess(
		ctx, s.clientConn, types.YdbConnectionParams{
			Endpoint:     schedule.DatabaseEndpoint,
			DatabaseName: schedule.DatabaseName,
		},
	); err != nil {
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

	err = s.driver.Apply(ctx, dbconnector.Changes{UpdateSchedules: []types.BackupSchedule{*schedule}})
	if err != nil {
		xlog.Error(
			ctx, "can't update backup schedule", zap.String(log_keys.BackupSchedule, schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't update backup schedule")
	}

	if schedule.Status == types.BackupScheduleStateInactive {
		metrics.GlobalMetricsRegistry.ResetScheduleCounters(schedule)
	}

	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.BackupSchedule, schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) DeleteBackupSchedule(
	ctx context.Context, request *pb.DeleteBackupScheduleRequest,
) (_ *pb.BackupSchedule, responseErr error) {
	const methodName string = "DeleteBackupSchedule"

	scheduleID := request.GetId()
	ctx = xlog.With(ctx, zap.String(log_keys.ScheduleID, scheduleID))

	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.Request, request))

	schedule, err := s.driver.GetScheduleWithBackupInfo(ctx, scheduleID)

	if err != nil && !errors.Is(err, dbconnector.ErrNotFound) {
		xlog.Error(ctx, "error getting backup schedule", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backup schedule")
	}
	if errors.Is(err, dbconnector.ErrNotFound) {
		xlog.Error(ctx, "backup schedule not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup schedule not found")
	}

	ctx = schedule.SetLogFields(ctx)
	// TODO: Need to check access to backup schedule not by container id?
	audit.SetAuditFieldsForRequest(
		ctx, &audit.AuditFields{ContainerID: schedule.ContainerID, Database: schedule.DatabaseName},
	)
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, schedule.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String(log_keys.Subject, subject))

	if schedule.Status == types.BackupScheduleStateDeleted {
		xlog.Error(ctx, "backup schedule already deleted")
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Error(codes.FailedPrecondition, "backup schedule already deleted")
	}

	schedule.Status = types.BackupScheduleStateDeleted
	err = s.driver.Apply(ctx, dbconnector.Changes{UpdateSchedules: []types.BackupSchedule{*schedule}})
	if err != nil {
		xlog.Error(
			ctx, "can't delete backup schedule", zap.String(log_keys.BackupSchedule, schedule.Proto(s.clock).String()),
			zap.Error(err),
		)
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't delete backup schedule")
	}

	metrics.GlobalMetricsRegistry.ResetScheduleCounters(schedule)

	xlog.Debug(ctx, methodName, zap.Stringer(log_keys.BackupSchedule, schedule))
	s.IncApiCallsCounter(methodName, codes.OK)
	return schedule.Proto(s.clock), nil
}

func (s *BackupScheduleService) Register(server server.Server) {
	pb.RegisterBackupScheduleServiceServer(server.GRPCServer(), s)
}

func NewBackupScheduleService(
	driver dbconnector.DBConnector,
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
