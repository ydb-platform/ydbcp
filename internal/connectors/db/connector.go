package db

import (
	"context"
	"errors"
	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/types"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"go.uber.org/zap"

	"ydbcp/internal/util/xlog"
)

var (
	readTx = table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	writeTx = table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
)

var ErrUnimplemented = errors.New("unimplemented")

type DBConnector interface {
	GetTableClient() table.Client
	SelectBackups(ctx context.Context, queryBuilder queries.ReadTableQuery) (
		[]*types.Backup, error,
	)
	SelectOperations(ctx context.Context, queryBuilder queries.ReadTableQuery) (
		[]types.Operation, error,
	)
	SelectBackupsByStatus(ctx context.Context, backupStatus string) ([]*types.Backup, error)
	ActiveOperations(context.Context) ([]types.Operation, error)
	UpdateOperation(context.Context, types.Operation) error
	CreateOperation(context.Context, types.Operation) (types.ObjectID, error)
	CreateBackup(context.Context, types.Backup) (types.ObjectID, error)
	UpdateBackup(context context.Context, id types.ObjectID, backupState string) error
	ExecuteUpsert(ctx context.Context, queryBuilder queries.WriteTableQuery) error
	Close()
}

type YdbConnector struct {
	driver *ydb.Driver
}

func NewYdbConnector(config config.Config) *YdbConnector {
	p := new(YdbConnector)
	p.driver = InitDriver(config.YdbcpDbConnectionString)
	return p
}

func InitDriver(dsn string) *ydb.Driver {
	ctx := context.Background()

	opts := []ydb.Option{
		ydb.WithAnonymousCredentials(),
	}
	xlog.Info(ctx, "connecting to ydb", zap.String("dsn", dsn))
	db, err := ydb.Open(ctx, dsn, opts...)

	if err != nil {
		// handle a connection error
		xlog.Error(
			ctx, "Error connecting to YDB", zap.String("message", err.Error()),
		)
		return nil
	}

	return db
}

func (d *YdbConnector) GetTableClient() table.Client {
	return d.driver.Table()
}

func (d *YdbConnector) Close() {
	ctx := context.Background()
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
	err = d.GetTableClient().Do(
		ctx, func(ctx context.Context, s table.Session) error {
			var (
				res result.Result
			)
			_, res, err = s.Execute(
				ctx,
				readTx,
				queryFormat.QueryText, queryFormat.QueryParams,
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
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					entity, readErr := readLambda(res)
					if readErr != nil {
						return readErr
					}
					entities = append(entities, entity)
				}
			}
			return res.Err()
		},
	)
	if err != nil {
		xlog.Error(
			ctx, "Error executing query", zap.String("message", err.Error()),
		)
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
	err = d.GetTableClient().Do(
		ctx, func(ctx context.Context, s table.Session) error {
			var (
				res result.Result
			)
			_, res, err = s.Execute(
				ctx,
				readTx,
				queryFormat.QueryText, queryFormat.QueryParams,
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
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					entity, readErr := readLambda(res)
					if readErr != nil {
						return readErr
					}
					entities = append(entities, entity)
				}
			}
			return res.Err()
		},
	)
	if err != nil {
		xlog.Error(
			ctx, "Error executing query", zap.String("message", err.Error()),
		)
		return nil, err
	}
	return entities, nil
}

func (d *YdbConnector) ExecuteUpsert(ctx context.Context, queryBuilder queries.WriteTableQuery) error {
	queryFormat, err := queryBuilder.FormatQuery(ctx)
	if err != nil {
		return err
	}
	err = d.GetTableClient().Do(
		ctx, func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				writeTx,
				queryFormat.QueryText,
				queryFormat.QueryParams,
			)
			if err != nil {
				return err
			}
			return nil
		},
	)
	if err != nil {
		xlog.Error(
			ctx, "Error executing query", zap.String("message", err.Error()),
		)
		return err
	}
	return nil
}

func (d *YdbConnector) SelectBackupsByStatus(
	ctx context.Context, backupStatus string,
) ([]*types.Backup, error) {
	return DoStructSelect[types.Backup](
		ctx,
		d,
		queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields("id"),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "status",
					Values: []table_types.Value{table_types.StringValueFromString(backupStatus)},
				},
			),
		),
		ReadBackupFromResultSet,
	)
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

func (d *YdbConnector) ActiveOperations(ctx context.Context) (
	[]types.Operation, error,
) {
	return DoInterfaceSelect[types.Operation](
		ctx,
		d,
		queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithSelectFields(queries.AllOperationFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field: "status",
					Values: []table_types.Value{
						table_types.StringValueFromString(string(types.OperationStatePending)),
						table_types.StringValueFromString(string(types.OperationStateCancelling)),
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
	return d.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithUpdateOperation(operation))
}

func (d *YdbConnector) CreateOperation(
	ctx context.Context, operation types.Operation,
) (types.ObjectID, error) {
	operation.SetId(types.GenerateObjectID())
	err := d.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateOperation(operation))
	if err != nil {
		return types.ObjectID{}, err
	}
	return operation.GetId(), nil
}

func (d *YdbConnector) CreateBackup(
	ctx context.Context, backup types.Backup,
) (types.ObjectID, error) {
	id := types.GenerateObjectID()
	backup.ID = id
	err := d.ExecuteUpsert(ctx, queries.NewWriteTableQuery().WithCreateBackup(backup))
	if err != nil {
		return types.ObjectID{}, err
	}
	return id, nil
}

func (d *YdbConnector) UpdateBackup(
	context context.Context, id types.ObjectID, backupStatus string,
) error {
	backup := types.Backup{
		ID:     id,
		Status: backupStatus,
	}
	return d.ExecuteUpsert(context, queries.NewWriteTableQuery().WithUpdateBackup(backup))
}
