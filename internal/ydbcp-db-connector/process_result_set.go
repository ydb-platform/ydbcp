package ydbcp_db_connector

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"ydbcp/internal/types"
)

type ReadResultSet[T any] func(result result.Result) (*T, error)

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

func ReadOperationFromResultSet(res result.Result) (*types.Operation, error) {
	var (
		operationId types.ObjectID
	)
	err := res.ScanNamed(
		named.Required("id", &operationId),
	)
	if err != nil {
		return nil, err
	}
	return &types.Operation{Id: operationId}, nil
}
