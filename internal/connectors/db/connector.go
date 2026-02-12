package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	ydbPrometheus "github.com/ydb-platform/ydb-go-sdk-prometheus/v2"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/credentials"
	"ydbcp/internal/metrics"
	"ydbcp/internal/types"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
)

var (
	readTx = query.TxControl(
		query.BeginTx(
			query.WithOnlineReadOnly(),
		),
		query.CommitTx(),
	)
	writeTx = query.TxControl(
		query.BeginTx(
			query.WithSerializableReadWrite(),
		),
		query.CommitTx(),
	)
)

type DBConnector interface {
	GetQueryClient() query.Client
	SelectBackups(ctx context.Context, queryBuilder queries.ReadTableQuery) (
		[]*types.Backup, error,
	)
	SelectOperations(ctx context.Context, queryBuilder queries.ReadTableQuery) (
		[]types.Operation, error,
	)

	SelectBackupSchedules(ctx context.Context, queryBuilder queries.ReadTableQuery) ([]*types.BackupSchedule, error)
	SelectBackupSchedulesWithRPOInfo(ctx context.Context, queryBuilder queries.ReadTableQuery) (
		[]*types.BackupSchedule, error,
	)
	ActiveOperations(context.Context) ([]types.Operation, error)
	UpdateOperation(context.Context, types.Operation) error
	CreateOperation(context.Context, types.Operation) (string, error)
	CreateBackup(context.Context, types.Backup) (string, error)
	UpdateBackup(context context.Context, id string, backupState string) error
	ExecuteUpsert(ctx context.Context, queryBuilder queries.WriteTableQuery) error
	Close(context.Context)
}

type YdbConnector struct {
	driver *ydb.Driver
}

func select1(baseCtx context.Context, db *ydb.Driver, timeout time.Duration) error {
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	ctx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()

	return db.Table().Do(
		ctx, func(ctx context.Context, s table.Session) error {
			_, res, err := s.Execute(
				ctx,
				readTx,
				"SELECT 1",
				nil,
			)
			if err != nil {
				return err
			}
			defer func(res result.Result) {
				err = res.Close()
				if err != nil {
					xlog.Error(ctx, "Error closing transaction result")
				}
			}(res) // result must be closed
			if res.ResultSetCount() != 1 {
				return errors.New("expected 1 result set")
			}
			return res.Err()
		},
	)
}

func NewYdbConnector(ctx context.Context, config config.YDBConnectionConfig) (*YdbConnector, error) {
	dialTimeout := time.Second * time.Duration(config.DialTimeoutSeconds)
	opts := []ydb.Option{
		ydb.WithDialTimeout(dialTimeout),
	}
	if config.Insecure {
		opts = append(opts, ydb.WithTLSSInsecureSkipVerify())
	}
	if !config.Discovery {
		opts = append(opts, ydb.WithBalancer(balancers.SingleConn()))
	}
	// Auth options (mutually exclusive)
	if config.K8sJWTAuth != nil {
		// K8s projected volume JWT authentication
		creds, err := credentials.NewK8sJWTCredentials(credentials.K8sJWTConfig{
			K8sTokenPath:         config.K8sJWTAuth.K8sTokenPath,
			TokenServiceEndpoint: config.K8sJWTAuth.TokenServiceEndpoint,
			SubjectToken:         config.K8sJWTAuth.SubjectToken,
			SubjectTokenType:     config.K8sJWTAuth.SubjectTokenType,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create K8s JWT credentials: %w", err)
		}
		opts = append(opts, ydb.WithCredentials(creds))
	} else if len(config.OAuth2KeyFile) > 0 {
		// OAuth2 key file authentication
		opts = append(opts, ydb.WithOauth2TokenExchangeCredentialsFile(config.OAuth2KeyFile))
	} else {
		opts = append(opts, ydb.WithAnonymousCredentials())
	}
	if config.EnableSDKMetrics {
		opts = append(opts, ydbPrometheus.WithTraces(metrics.GlobalMetricsRegistry.GetReg()))
	}

	xlog.Info(ctx, "connecting to ydb", zap.String(log_keys.ClientDSN, config.ConnectionString))
	driver, err := ydb.Open(ctx, config.ConnectionString, opts...)
	if err != nil {
		return nil, fmt.Errorf("can't connect to YDB, dsn %s: %w", config.ConnectionString, err)
	}
	err = select1(ctx, driver, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("can't connect to YDB, dsn %s: %w", config.ConnectionString, err)
	}

	return &YdbConnector{driver: driver}, nil
}

func (d *YdbConnector) GRPCConn() grpc.ClientConnInterface {
	return ydb.GRPCConn(d.driver)
}

func (d *YdbConnector) GetQueryClient() query.Client {
	return d.driver.Query()
}

func (d *YdbConnector) Close(ctx context.Context) {
	err := d.driver.Close(ctx)
	if err != nil {
		xlog.Error(ctx, "Error closing YDB driver")
	}
}

func DoStructSelect[T any](
	ctx context.Context,
	d *YdbConnector,
	queryBuilder queries.ReadTableQuery,
	readLambda StructFromResultSet[T],
) ([]*T, error) {
	var (
		entities []*T
	)
	queryFormat, err := queryBuilder.FormatQuery(ctx)
	if err != nil {
		return nil, err
	}
	err = d.GetQueryClient().Do(
		ctx, func(ctx context.Context, s query.Session) error {
			var (
				res query.Result
			)
			res, err = s.Query(
				ctx,
				queryFormat.QueryText,
				query.WithParameters(queryFormat.QueryParams),
				query.WithTxControl(readTx),
			)
			if err != nil {
				return err
			}
			defer func(res query.Result) {
				err = res.Close(ctx)
				if err != nil {
					xlog.Error(ctx, "Error closing transaction result")
				}
			}(res) // result must be closed

			resultSetCount := 0
			for {
				resultSet, err := res.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}

				resultSetCount++
				if resultSetCount > 1 {
					return errors.New("expected 1 result set")
				}

				for {
					row, err := resultSet.NextRow(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						return err
					}

					entity, readErr := readLambda(row)
					if readErr != nil {
						xlog.Error(ctx, "Error reading row", zap.Error(readErr))
						metrics.GlobalMetricsRegistry.IncYdbErrorsCounter()
					} else {
						entities = append(entities, entity)
					}
				}
			}
			return nil
		},
	)
	if err != nil {
		xlog.Error(ctx, "Error executing query", zap.Error(err))
		metrics.GlobalMetricsRegistry.IncYdbErrorsCounter()
		return nil, err
	}
	return entities, nil
}

func DoInterfaceSelect[T any](
	ctx context.Context,
	d *YdbConnector,
	queryBuilder queries.ReadTableQuery,
	readLambda InterfaceFromResultSet[T],
) ([]T, error) {
	var (
		entities []T
	)
	queryFormat, err := queryBuilder.FormatQuery(ctx)
	if err != nil {
		return nil, err
	}
	err = d.GetQueryClient().Do(
		ctx, func(ctx context.Context, s query.Session) error {
			var (
				res query.Result
			)
			res, err = s.Query(
				ctx,
				queryFormat.QueryText,
				query.WithParameters(queryFormat.QueryParams),
				query.WithTxControl(readTx),
			)
			if err != nil {
				return err
			}
			defer func(res query.Result) {
				err = res.Close(ctx)
				if err != nil {
					xlog.Error(ctx, "Error closing transaction result")
				}
			}(res) // result must be closed

			resultSetCount := 0
			for {
				resultSet, err := res.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}

				resultSetCount++
				if resultSetCount > 1 {
					return errors.New("expected 1 result set")
				}

				for {
					row, err := resultSet.NextRow(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						return err
					}

					entity, readErr := readLambda(row)
					if readErr != nil {
						xlog.Error(ctx, "Error reading row", zap.Error(readErr))
						metrics.GlobalMetricsRegistry.IncYdbErrorsCounter()
					} else {
						entities = append(entities, entity)
					}
				}
			}
			return nil
		},
	)
	if err != nil {
		xlog.Error(ctx, "Error executing query", zap.Error(err))
		metrics.GlobalMetricsRegistry.IncYdbErrorsCounter()
		return nil, err
	}
	return entities, nil
}

func (d *YdbConnector) ExecuteUpsert(ctx context.Context, queryBuilder queries.WriteTableQuery) error {
	queryFormat, err := queryBuilder.FormatQuery(ctx)
	if err != nil {
		return err
	}
	err = d.GetQueryClient().Do(
		ctx, func(ctx context.Context, s query.Session) (err error) {
			res, err := s.Query(
				ctx,
				queryFormat.QueryText,
				query.WithParameters(queryFormat.QueryParams),
				query.WithTxControl(writeTx),
			)
			if err != nil {
				return err
			}
			defer func(res query.Result) {
				err = res.Close(ctx)
				if err != nil {
					xlog.Error(ctx, "Error closing transaction result")
				}
			}(res) // result must be closed
			return nil
		},
	)
	if err != nil {
		xlog.Error(ctx, "Error executing query", zap.Error(err))
		metrics.GlobalMetricsRegistry.IncYdbErrorsCounter()
		return err
	}
	if ops := queryBuilder.GetOperations(); len(ops) > 0 {
		for _, op := range ops {
			metrics.GlobalMetricsRegistry.IncOperationsStartedCounter(op)
		}
	}
	return nil
}

func (d *YdbConnector) SelectBackups(
	ctx context.Context, queryBuilder queries.ReadTableQuery,
) ([]*types.Backup, error) {
	return DoStructSelect[types.Backup](
		ctx,
		d,
		queryBuilder,
		ReadBackupFromResultSet,
	)
}

func (d *YdbConnector) SelectOperations(
	ctx context.Context, queryBuilder queries.ReadTableQuery,
) ([]types.Operation, error) {
	return DoInterfaceSelect[types.Operation](
		ctx,
		d,
		queryBuilder,
		ReadOperationFromResultSet,
	)
}

func (d *YdbConnector) SelectBackupSchedules(
	ctx context.Context, queryBuilder queries.ReadTableQuery,
) ([]*types.BackupSchedule, error) {
	return DoStructSelect[types.BackupSchedule](
		ctx,
		d,
		queryBuilder,
		func(res query.Row) (*types.BackupSchedule, error) {
			return ReadBackupScheduleFromResultSet(res, false)
		},
	)
}

func (d *YdbConnector) SelectBackupSchedulesWithRPOInfo(
	ctx context.Context, queryBuilder queries.ReadTableQuery,
) ([]*types.BackupSchedule, error) {
	return DoStructSelect[types.BackupSchedule](
		ctx,
		d,
		queryBuilder,
		func(res query.Row) (*types.BackupSchedule, error) {
			return ReadBackupScheduleFromResultSet(res, true)
		},
	)
}

func (d *YdbConnector) ActiveOperations(ctx context.Context) (
	[]types.Operation, error,
) {
	return DoInterfaceSelect[types.Operation](
		ctx,
		d,
		queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field: "status",
					Values: []table_types.Value{
						table_types.StringValueFromString(types.OperationStatePending.String()),
						table_types.StringValueFromString(types.OperationStateRunning.String()),
						table_types.StringValueFromString(types.OperationStateCancelling.String()),
						table_types.StringValueFromString(types.OperationStateStartCancelling.String()),
					},
				},
			),
		),
		ReadOperationFromResultSet,
	)
}

func (d *YdbConnector) UpdateOperation(
	ctx context.Context, operation types.Operation,
) error {
	if operation.GetAudit() != nil && operation.GetAudit().CompletedAt != nil {
		operation.SetUpdatedAt(operation.GetAudit().CompletedAt)
	} else {
		operation.SetUpdatedAt(timestamppb.Now())
	}

	return d.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateOperation(operation))
}

func (d *YdbConnector) CreateOperation(
	ctx context.Context, operation types.Operation,
) (string, error) {
	operation.SetID(types.GenerateObjectID())
	err := d.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateOperation(operation))
	if err != nil {
		return "", err
	}
	return operation.GetID(), nil
}

func (d *YdbConnector) CreateBackup(
	ctx context.Context, backup types.Backup,
) (string, error) {
	id := types.GenerateObjectID()
	backup.ID = id
	err := d.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateBackup(backup))
	if err != nil {
		return "", err
	}
	return id, nil
}

func (d *YdbConnector) UpdateBackup(
	ctx context.Context, id string, backupStatus string,
) error {
	backup := types.Backup{
		ID:     id,
		Status: backupStatus,
	}
	return d.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateBackup(backup))
}
