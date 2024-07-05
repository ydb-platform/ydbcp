package ydbcp_db_connector

import (
	"context"
	"errors"
	"ydbcp/internal/config"
	"ydbcp/internal/types"
	"ydbcp/internal/yql/queries"

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
)

var ErrUnimplemented = errors.New("unimplemented")

type YdbDriver interface {
	GetTableClient() table.Client
	SelectBackups(ctx context.Context, backupStatus string) ([]types.Backup, error)
	ActiveOperations(context.Context) ([]types.Operation, error)
	UpdateOperation(context.Context, types.Operation) error
	CreateOperation(context.Context, types.Operation) (types.ObjectID, error)
	Close()
}

type YdbDriverImpl struct {
	ydbDriver *ydb.Driver
}

func NewYdbDriver(config config.Config) *YdbDriverImpl {
	p := new(YdbDriverImpl)
	p.ydbDriver = InitDriver(config.YdbcpDbConnectionString)
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
		xlog.Error(ctx, "Error connecting to YDB", zap.String("message", err.Error()))
		return nil
	}

	return db
}

func (d YdbDriverImpl) GetTableClient() table.Client {
	return d.ydbDriver.Table()
}

func (d YdbDriverImpl) Close() {
	ctx := context.Background()
	err := d.ydbDriver.Close(ctx)
	if err != nil {
		xlog.Error(ctx, "Error closing YDB driver")
	}
}

func DoEntitySelect[T any](ctx context.Context, d YdbDriver, query string, readLambda ReadResultSet[T]) ([]T, error) {
	var (
		entities []T
	)
	err := d.GetTableClient().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		var (
			res result.Result
		)
		_, res, err = s.Execute(
			ctx,
			readTx,
			query, table.NewQueryParameters())
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
				entities = append(entities, *entity)
			}
		}
		return res.Err()
	})
	if err != nil {
		xlog.Error(ctx, "Error executing query", zap.String("message", err.Error()))
		return nil, err
	}
	return entities, nil
}

func (d YdbDriverImpl) SelectBackups(ctx context.Context, backupStatus string) ([]types.Backup, error) {
	return DoEntitySelect[types.Backup](
		ctx,
		d,
		queries.SelectEntitiesQuery("Backups", backupStatus),
		ReadBackupFromResultSet,
	)
}

func (d YdbDriverImpl) ActiveOperations(ctx context.Context) ([]types.Operation, error) {
	return DoEntitySelect[types.Operation](
		ctx,
		d,
		queries.SelectEntitiesQuery("Operations", types.StatePending, types.StateCancelling),
		ReadOperationFromResultSet,
	)
}

func (d YdbDriverImpl) UpdateOperation(context.Context, types.Operation) error {
	return ErrUnimplemented
}

func (d YdbDriverImpl) CreateOperation(context.Context, types.Operation) (types.ObjectID, error) {
	return types.GenerateObjectID(), ErrUnimplemented
}
