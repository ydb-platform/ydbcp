package db

import (
	"fmt"
	"strings"
	"time"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StructFromResultSet[T any] func(result result.Result) (*T, error)

type InterfaceFromResultSet[T any] func(result result.Result) (T, error)

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

func ReadBackupFromResultSet(res result.Result) (*types.Backup, error) {
	var (
		backupId         string
		containerId      string
		databaseName     string
		databaseEndpoint string
		s3endpoint       *string
		s3region         *string
		s3bucket         *string
		s3pathprefix     *string
		status           *string
		message          *string
		size             *int64

		creator     *string
		completedAt *time.Time
		createdAt   *time.Time
	)

	err := res.ScanNamed(
		named.Required("id", &backupId),
		named.Required("container_id", &containerId),
		named.Required("database", &databaseName),
		named.Required("endpoint", &databaseEndpoint),
		named.Optional("s3_endpoint", &s3endpoint),
		named.Optional("s3_region", &s3region),
		named.Optional("s3_bucket", &s3bucket),
		named.Optional("s3_path_prefix", &s3pathprefix),
		named.Optional("status", &status),
		named.Optional("message", &message),
		named.Optional("size", &size),

		named.Optional("created_at", &createdAt),
		named.Optional("completed_at", &completedAt),
		named.Optional("initiated", &creator),
	)
	if err != nil {
		return nil, err
	}

	return &types.Backup{
		ID:               backupId,
		ContainerID:      containerId,
		DatabaseName:     databaseName,
		DatabaseEndpoint: databaseEndpoint,
		S3Endpoint:       StringOrEmpty(s3endpoint),
		S3Region:         StringOrEmpty(s3region),
		S3Bucket:         StringOrEmpty(s3bucket),
		S3PathPrefix:     StringOrEmpty(s3pathprefix),
		Status:           StringOrDefault(status, types.BackupStateUnknown),
		Message:          StringOrEmpty(message),
		AuditInfo:        auditFromDb(creator, createdAt, completedAt),
		Size:             Int64OrZero(size),
	}, nil
}

func ReadOperationFromResultSet(res result.Result) (types.Operation, error) {
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
		sourcePaths          *string
		sourcePathsToExclude *string
		creator              *string
		createdAt            *time.Time
		completedAt          *time.Time
	)
	err := res.ScanNamed(
		named.Required("id", &operationId),
		named.Required("container_id", &containerId),
		named.Required("type", &operationType),
		named.Required("database", &databaseName),
		named.Required("endpoint", &databaseEndpoint),

		named.Optional("backup_id", &backupId),
		named.Optional("operation_id", &ydbOperationId),
		named.Optional("status", &operationStateBuf),
		named.Optional("message", &message),
		named.Optional("paths", &sourcePaths),
		named.Optional("paths_to_exclude", &sourcePathsToExclude),

		named.Optional("created_at", &createdAt),
		named.Optional("completed_at", &completedAt),
		named.Optional("initiated", &creator),
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
		sourcePathsSlice = strings.Split(*sourcePaths, ",")
	}
	if sourcePathsToExclude != nil {
		sourcePathsToExcludeSlice = strings.Split(*sourcePathsToExclude, ",")
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
			SourcePaths:          sourcePathsSlice,
			SourcePathsToExclude: sourcePathsToExcludeSlice,
			YdbOperationId:       StringOrEmpty(ydbOperationId),
			Audit:                auditFromDb(creator, createdAt, completedAt),
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
		}, nil
	}

	return &types.GenericOperation{ID: operationId}, nil
}

func ReadBackupScheduleFromResultSet(res result.Result) (*types.BackupSchedule, error) {
	var (
		ID               string
		containerID      string
		databaseName     string
		databaseEndpoint string
		active           bool

		crontab string

		initiated              *string
		createdAt              *time.Time
		name                   *string
		ttl                    *time.Duration
		sourcePaths            *string
		sourcePathsToExclude   *string
		recoveryPointObjective *time.Duration
		lastBackupID           *string
		lastSuccessfulBackupID *string
		recoveryPoint          *time.Time
		nextLaunch             *time.Time
	)
	err := res.ScanNamed(
		named.Required("id", &ID),
		named.Required("container_id", &containerID),
		named.Required("database", &databaseName),
		named.Required("endpoint", &databaseEndpoint),
		named.Required("active", &active),
		named.Required("crontab", &crontab),

		named.Optional("initiated", &initiated),
		named.Optional("created_at", &createdAt),
		named.Optional("name", &name),
		named.Optional("ttl", &ttl),
		named.Optional("paths", &sourcePaths),
		named.Optional("paths_to_exclude", &sourcePathsToExclude),
		named.Optional("recovery_point_objective", &recoveryPointObjective),
		named.Optional("last_backup_id", &lastBackupID),
		named.Optional("last_successful_backup_id", &lastSuccessfulBackupID),
		named.Optional("recovery_point", &recoveryPoint),
		named.Optional("next_launch", &nextLaunch),
	)
	if err != nil {
		return nil, err
	}

	sourcePathsSlice := make([]string, 0)
	sourcePathsToExcludeSlice := make([]string, 0)
	if sourcePaths != nil {
		sourcePathsSlice = strings.Split(*sourcePaths, ",")
	}
	if sourcePathsToExclude != nil {
		sourcePathsToExcludeSlice = strings.Split(*sourcePathsToExclude, ",")
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
		SourcePaths:          sourcePathsSlice,
		SourcePathsToExclude: sourcePathsToExcludeSlice,
		Audit:                auditFromDb(initiated, createdAt, nil),
		Name:                 StringOrEmpty(name),
		Active:               active,
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
