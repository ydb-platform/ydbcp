package db

import (
	"fmt"
	"time"

	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StructFromResultSet[T any] func(result query.Row) (*T, error)

type InterfaceFromResultSet[T any] func(result query.Row) (T, error)

func StringOrDefault(str *string, def string) string {
	if str == nil {
		return def
	}
	return *str
}

func StringOrEmpty(str *string) string {
	return StringOrDefault(str, "")
}

func Int64OrZero(number *int64) int64 {
	if number == nil {
		return 0
	}
	return *number
}

func auditFromDb(initiated *string, createdAt *time.Time, completedAt *time.Time) *pb.AuditInfo {
	var createdTs *timestamppb.Timestamp
	var completedTs *timestamppb.Timestamp
	creatorStr := ""
	if initiated != nil {
		creatorStr = *initiated
	}
	createdTs = nil
	completedTs = nil
	if createdAt != nil {
		createdTs = timestamppb.New(*createdAt)
	}
	if completedAt != nil {
		completedTs = timestamppb.New(*completedAt)
	}
	return &pb.AuditInfo{
		Creator:     creatorStr,
		CreatedAt:   createdTs,
		CompletedAt: completedTs,
	}
}

//TODO: unit test this

func ReadBackupFromResultSet(res query.Row) (*types.Backup, error) {
	var (
		backupId         string
		containerId      string
		databaseName     string
		databaseEndpoint string
		s3endpoint       *string
		s3region         *string
		s3bucket         *string
		s3pathPrefix     *string
		status           *string
		message          *string
		size             *int64
		scheduleId       *string
		sourcePaths      *string

		creator     *string
		completedAt *time.Time
		createdAt   *time.Time
		expireAt    *time.Time
	)

	err := res.ScanNamed(
		query.Named("id", &backupId),
		query.Named("container_id", &containerId),
		query.Named("database", &databaseName),
		query.Named("endpoint", &databaseEndpoint),
		query.Named("s3_endpoint", &s3endpoint),
		query.Named("s3_region", &s3region),
		query.Named("s3_bucket", &s3bucket),
		query.Named("s3_path_prefix", &s3pathPrefix),
		query.Named("status", &status),
		query.Named("message", &message),
		query.Named("size", &size),
		query.Named("schedule_id", &scheduleId),
		query.Named("expire_at", &expireAt),
		query.Named("paths", &sourcePaths),

		query.Named("created_at", &createdAt),
		query.Named("completed_at", &completedAt),
		query.Named("initiated", &creator),
	)
	if err != nil {
		return nil, err
	}
	sourcePathsSlice := make([]string, 0)

	if sourcePaths != nil {
		sourcePathsSlice, err = types.ParseSourcePaths(*sourcePaths)
		if err != nil {
			return nil, err
		}
	}

	return &types.Backup{
		ID:               backupId,
		ContainerID:      containerId,
		DatabaseName:     databaseName,
		DatabaseEndpoint: databaseEndpoint,
		S3Endpoint:       StringOrEmpty(s3endpoint),
		S3Region:         StringOrEmpty(s3region),
		S3Bucket:         StringOrEmpty(s3bucket),
		S3PathPrefix:     StringOrEmpty(s3pathPrefix),
		Status:           StringOrDefault(status, types.BackupStateUnknown),
		Message:          StringOrEmpty(message),
		AuditInfo:        auditFromDb(creator, createdAt, completedAt),
		Size:             Int64OrZero(size),
		ScheduleID:       scheduleId,
		ExpireAt:         expireAt,
		SourcePaths:      sourcePathsSlice,
	}, nil
}

func ReadOperationFromResultSet(res query.Row) (types.Operation, error) {
	var (
		operationId      string
		containerId      string
		operationType    string
		databaseName     string
		databaseEndpoint string

		backupId             *string
		ydbOperationId       *string
		operationStateBuf    *string
		message              *string
		rootPath             *string
		sourcePaths          *string
		sourcePathsToExclude *string
		creator              *string
		createdAt            *time.Time
		completedAt          *time.Time
		updatedAt            *time.Time
		updatedTs            *timestamppb.Timestamp
		parentOperationID    *string
		scheduleID           *string
		ttl                  *time.Duration
		retries              *uint32
		retriesCount         *uint32
		maxBackoff           *time.Duration
	)
	err := res.ScanNamed(
		query.Named("id", &operationId),
		query.Named("container_id", &containerId),
		query.Named("type", &operationType),
		query.Named("database", &databaseName),
		query.Named("endpoint", &databaseEndpoint),

		query.Named("backup_id", &backupId),
		query.Named("operation_id", &ydbOperationId),
		query.Named("status", &operationStateBuf),
		query.Named("message", &message),
		query.Named("root_path", &rootPath),
		query.Named("paths", &sourcePaths),
		query.Named("paths_to_exclude", &sourcePathsToExclude),

		query.Named("created_at", &createdAt),
		query.Named("completed_at", &completedAt),
		query.Named("initiated", &creator),
		query.Named("updated_at", &updatedAt),
		query.Named("parent_operation_id", &parentOperationID),
		query.Named("schedule_id", &scheduleID),
		query.Named("ttl", &ttl),
		query.Named("retries", &retries),
		query.Named("retries_count", &retriesCount),
		query.Named("retries_max_backoff", &maxBackoff),
	)
	if err != nil {
		return nil, err
	}
	operationState := types.OperationStateUnknown
	if operationStateBuf != nil {
		operationState = types.OperationState(*operationStateBuf)
	}
	sourcePathsSlice := make([]string, 0)
	sourcePathsToExcludeSlice := make([]string, 0)
	if sourcePaths != nil {
		sourcePathsSlice, err = types.ParseSourcePaths(*sourcePaths)
		if err != nil {
			return nil, err
		}

	}
	if sourcePathsToExclude != nil {
		sourcePathsToExcludeSlice, err = types.ParseSourcePaths(*sourcePathsToExclude)
		if err != nil {
			return nil, err
		}
	}

	if updatedAt != nil {
		updatedTs = timestamppb.New(*updatedAt)
	}

	if operationType == string(types.OperationTypeTB) {
		if backupId == nil {
			return nil, fmt.Errorf("failed to read backup_id for TB operation: %s", operationId)
		}
		return &types.TakeBackupOperation{
			ID:          operationId,
			BackupID:    *backupId,
			ContainerID: containerId,
			State:       operationState,
			Message:     StringOrEmpty(message),
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     databaseEndpoint,
				DatabaseName: databaseName,
			},
			RootPath:             StringOrEmpty(rootPath),
			SourcePaths:          sourcePathsSlice,
			SourcePathsToExclude: sourcePathsToExcludeSlice,
			YdbOperationId:       StringOrEmpty(ydbOperationId),
			Audit:                auditFromDb(creator, createdAt, completedAt),
			UpdatedAt:            updatedTs,
			ParentOperationID:    parentOperationID,
		}, nil
	} else if operationType == string(types.OperationTypeRB) {
		if backupId == nil {
			return nil, fmt.Errorf("failed to read backup_id for RB operation: %s", operationId)
		}
		return &types.RestoreBackupOperation{
			ID:          operationId,
			BackupId:    *backupId,
			ContainerID: containerId,
			State:       operationState,
			Message:     StringOrEmpty(message),
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     databaseEndpoint,
				DatabaseName: databaseName,
			},
			YdbOperationId: StringOrEmpty(ydbOperationId),
			SourcePaths:    sourcePathsSlice,
			Audit:          auditFromDb(creator, createdAt, completedAt),
			UpdatedAt:      updatedTs,
		}, nil
	} else if operationType == string(types.OperationTypeDB) {
		if backupId == nil {
			return nil, fmt.Errorf("failed to read backup_id for DB operation: %s", operationId)
		}

		var pathPrefix string
		if len(sourcePathsSlice) > 0 {
			pathPrefix = sourcePathsSlice[0]
		}

		return &types.DeleteBackupOperation{
			ID:          operationId,
			BackupID:    *backupId,
			ContainerID: containerId,
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     databaseEndpoint,
				DatabaseName: databaseName,
			},
			State:      operationState,
			Message:    StringOrEmpty(message),
			Audit:      auditFromDb(creator, createdAt, completedAt),
			PathPrefix: pathPrefix,
			UpdatedAt:  updatedTs,
		}, nil
	} else if operationType == string(types.OperationTypeTBWR) {
		var retryConfig *pb.RetryConfig = nil
		if maxBackoff != nil {
			retryConfig = &pb.RetryConfig{
				Retries: &pb.RetryConfig_MaxBackoff{
					MaxBackoff: durationpb.New(*maxBackoff),
				},
			}
		}
		if retriesCount != nil {
			retryConfig = &pb.RetryConfig{
				Retries: &pb.RetryConfig_Count{Count: *retriesCount},
			}
		}
		retryNum := 0
		if retries != nil {
			retryNum = int(*retries)
		}
		return &types.TakeBackupWithRetryOperation{
			TakeBackupOperation: types.TakeBackupOperation{
				ID:          operationId,
				ContainerID: containerId,
				State:       operationState,
				Message:     StringOrEmpty(message),
				YdbConnectionParams: types.YdbConnectionParams{
					Endpoint:     databaseEndpoint,
					DatabaseName: databaseName,
				},
				RootPath:             StringOrEmpty(rootPath),
				SourcePaths:          sourcePathsSlice,
				SourcePathsToExclude: sourcePathsToExcludeSlice,
				Audit:                auditFromDb(creator, createdAt, completedAt),
				UpdatedAt:            updatedTs,
			},
			ScheduleID:  scheduleID,
			Ttl:         ttl,
			Retries:     retryNum,
			RetryConfig: retryConfig,
		}, nil
	}

	return &types.GenericOperation{ID: operationId}, nil
}

func ReadBackupScheduleFromResultSet(res query.Row, withRPOInfo bool) (*types.BackupSchedule, error) {
	var (
		ID               string
		containerID      string
		databaseName     string
		databaseEndpoint string

		crontab string

		status                 *string
		initiated              *string
		createdAt              *time.Time
		name                   *string
		ttl                    *time.Duration
		rootPath               *string
		sourcePaths            *string
		sourcePathsToExclude   *string
		recoveryPointObjective *time.Duration
		lastBackupID           *string
		lastSuccessfulBackupID *string
		recoveryPoint          *time.Time
		nextLaunch             *time.Time
	)

	namedValues := []query.NamedDestination{
		query.Named("id", &ID),
		query.Named("container_id", &containerID),
		query.Named("database", &databaseName),
		query.Named("endpoint", &databaseEndpoint),
		query.Named("crontab", &crontab),

		query.Named("status", &status),
		query.Named("initiated", &initiated),
		query.Named("created_at", &createdAt),
		query.Named("name", &name),
		query.Named("ttl", &ttl),
		query.Named("root_path", &rootPath),
		query.Named("paths", &sourcePaths),
		query.Named("paths_to_exclude", &sourcePathsToExclude),
		query.Named("recovery_point_objective", &recoveryPointObjective),
		query.Named("next_launch", &nextLaunch),
	}
	if withRPOInfo {
		namedValues = append(namedValues, query.Named("last_backup_id", &lastBackupID))
		namedValues = append(namedValues, query.Named("last_successful_backup_id", &lastSuccessfulBackupID))
		namedValues = append(namedValues, query.Named("recovery_point", &recoveryPoint))
	}

	err := res.ScanNamed(namedValues...)

	if err != nil {
		return nil, err
	}

	var sourcePathsSlice []string
	var sourcePathsToExcludeSlice []string
	if sourcePaths != nil {
		sourcePathsSlice, err = types.ParseSourcePaths(*sourcePaths)
		if err != nil {
			return nil, err
		}
	}
	if sourcePathsToExclude != nil {
		sourcePathsToExcludeSlice, err = types.ParseSourcePaths(*sourcePathsToExclude)
		if err != nil {
			return nil, err
		}
	}

	var ttlDuration *durationpb.Duration
	var rpoDuration *durationpb.Duration

	ttlDuration = nil
	rpoDuration = nil
	if ttl != nil {
		ttlDuration = durationpb.New(*ttl)
	}
	if recoveryPointObjective != nil {
		rpoDuration = durationpb.New(*recoveryPointObjective)
	}

	return &types.BackupSchedule{
		ID:                   ID,
		ContainerID:          containerID,
		DatabaseName:         databaseName,
		DatabaseEndpoint:     databaseEndpoint,
		RootPath:             StringOrEmpty(rootPath),
		SourcePaths:          sourcePathsSlice,
		SourcePathsToExclude: sourcePathsToExcludeSlice,
		Audit:                auditFromDb(initiated, createdAt, nil),
		Name:                 name,
		Status:               StringOrDefault(status, types.BackupScheduleStateUnknown),
		ScheduleSettings: &pb.BackupScheduleSettings{
			SchedulePattern:        &pb.BackupSchedulePattern{Crontab: crontab},
			Ttl:                    ttlDuration,
			RecoveryPointObjective: rpoDuration,
		},
		NextLaunch:             nextLaunch,
		LastBackupID:           lastBackupID,
		LastSuccessfulBackupID: lastSuccessfulBackupID,
		RecoveryPoint:          recoveryPoint,
	}, nil
}
