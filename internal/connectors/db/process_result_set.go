package db

import (
	"errors"
	"fmt"
	"ydbcp/internal/types"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

type StructFromResultSet[T any] func(result result.Result) (*T, error)

type InterfaceFromResultSet[T any] func(result result.Result) (T, error)

func ReadBackupFromResultSet(res result.Result) (*types.Backup, error) {
	var (
		backupId    types.ObjectID
		operationId types.ObjectID
	)

	err := res.ScanNamed(
		named.Required("id", &backupId),
		named.Optional("operation_id", &operationId),
	)
	if err != nil {
		return nil, err
	}
	return &types.Backup{Id: backupId, OperationId: operationId}, nil
}

func ReadOperationFromResultSet(res result.Result) (types.Operation, error) {
	var (
		operationId    types.ObjectID
		backupId       *types.ObjectID
		operationType  types.OperationType
		operationState string
		database       *string
		ydbOperationId *string
	)
	err := res.ScanNamed(
		named.Required("id", &operationId),
		named.Optional("backup_id", &backupId),
		named.Required("type", &operationType),
		named.Required("status", &operationState),
		named.Optional("operation_id", &ydbOperationId),
		named.Optional("database", &database),
	)
	if err != nil {
		return nil, err
	}
	if operationType == types.OperationType("TB") {
		if backupId == nil || database == nil || ydbOperationId == nil {
			return nil, errors.New(fmt.Sprintf("failed to read required fields of operation %s", operationId.String()))
		}
		return &types.TakeBackupOperation{
			Id:                  operationId,
			BackupId:            *backupId,
			Type:                operationType,
			State:               operationState,
			Message:             "",
			YdbConnectionParams: types.GetYdbConnectionParams(*database),
			YdbOperationId:      *ydbOperationId,
		}, nil
	}
	return &types.GenericOperation{Id: operationId}, nil
}
