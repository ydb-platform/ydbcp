package db

import (
	"fmt"
	"time"
	"ydbcp/internal/types"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
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

func ReadBackupFromResultSet(res result.Result) (*types.Backup, error) {
	var (
		backupId         [16]byte
		containerId      string
		databaseName     string
		databaseEndpoint string
		s3endpoint       *string
		s3region         *string
		s3bucket         *string
		s3pathprefix     *string
		status           *string
		message          *string
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
	)
	if err != nil {
		return nil, err
	}

	id, err := uuid.FromBytes(backupId[:])

	if err != nil {
		return nil, err
	}

	return &types.Backup{
		ID:               types.ObjectID(id),
		ContainerID:      containerId,
		DatabaseName:     databaseName,
		DatabaseEndpoint: databaseEndpoint,
		S3Endpoint:       StringOrEmpty(s3endpoint),
		S3Region:         StringOrEmpty(s3region),
		S3Bucket:         StringOrEmpty(s3bucket),
		S3PathPrefix:     StringOrEmpty(s3pathprefix),
		Status:           StringOrDefault(status, types.BackupStateUnknown),
		Message:          StringOrEmpty(message),
	}, nil
}

func ReadOperationFromResultSet(res result.Result) (types.Operation, error) {
	var (
		operationId      types.ObjectID
		containerId      string
		operationType    string
		createdAt        time.Time
		databaseName     string
		databaseEndpoint string

		backupId          *types.ObjectID
		ydbOperationId    *string
		operationStateBuf *string
		message           *string
	)
	err := res.ScanNamed(
		named.Required("id", &operationId),
		named.Required("container_id", &containerId),
		named.Required("type", &operationType),
		named.Required("created_at", &createdAt),
		named.Required("database", &databaseName),
		named.Required("endpoint", &databaseEndpoint),

		named.Optional("backup_id", &backupId),
		named.Optional("operation_id", &ydbOperationId),
		named.Optional("status", &operationStateBuf),
		named.Optional("message", &message),
	)
	if err != nil {
		return nil, err
	}
	operationState := types.OperationStateUnknown
	if operationStateBuf != nil {
		operationState = types.OperationState(*operationStateBuf)
	}
	if operationType == string(types.OperationTypeTB) {
		if backupId == nil {
			return nil, fmt.Errorf("failed to read backup_id for TB operation: %s", operationId.String())
		}
		return &types.TakeBackupOperation{
			Id:          operationId,
			BackupId:    *backupId,
			ContainerID: containerId,
			State:       operationState,
			Message:     StringOrEmpty(message),
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     databaseEndpoint,
				DatabaseName: databaseName,
			},
			YdbOperationId: StringOrEmpty(ydbOperationId),
			CreatedAt:      createdAt,
		}, nil
	} else if operationType == string(types.OperationTypeRB) {
		if backupId == nil {
			return nil, fmt.Errorf("failed to read backup_id for TB operation: %s", operationId.String())
		}
		return &types.RestoreBackupOperation{
			Id:          operationId,
			BackupId:    *backupId,
			ContainerID: containerId,
			State:       operationState,
			Message:     StringOrEmpty(message),
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     databaseEndpoint,
				DatabaseName: databaseName,
			},
			YdbOperationId: StringOrEmpty(ydbOperationId),
			CreatedAt:      createdAt,
		}, nil
	}

	return &types.GenericOperation{Id: operationId}, nil
}
