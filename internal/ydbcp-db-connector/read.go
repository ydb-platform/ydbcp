package ydbcp_db_connector

import (
	"context"
	"errors"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"go.uber.org/zap"
	"ydbcp/internal/config"
	"ydbcp/internal/types"
	"ydbcp/internal/yql/queries"

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

type YdbDriver interface {
	SelectBackups(ctx context.Context, backupStatus string) []types.Backup
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

func (d YdbDriverImpl) Close() {
	ctx := context.Background()
	err := d.ydbDriver.Close(ctx)
	if err != nil {
		xlog.Error(ctx, "Error closing YDB driver")
	}
}

func (d YdbDriverImpl) SelectBackups(ctx context.Context, backupStatus string) []types.Backup {
	var backups []types.Backup
	err := d.ydbDriver.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		var (
			res          result.Result
			backup_id    types.ObjectID
			operation_id *string
		)
		_, res, err = s.Execute(
			ctx,
			readTx,
			queries.SelectBackupsQuery(backupStatus), table.NewQueryParameters())
		if err != nil {
			return err
		}
		defer func(res result.Result) {
			err := res.Close()
			if err != nil {
				xlog.Error(ctx, "Error closing transaction result")
			}
		}(res) // result must be closed
		if res.ResultSetCount() != 1 {
			return errors.New("expected 1 result set")
		}
		for res.NextResultSet(ctx) {
			for res.NextRow() {
				err = res.ScanNamed(
					named.Required("id", &backup_id),
					named.Optional("operation_id", &operation_id),
				)
				if err != nil {
					return err
				}
				backups = append(backups, types.Backup{Backup_id: backup_id, Operation_id: operation_id})
			}
		}
		return res.Err()
	})
	if err != nil {
		xlog.Error(ctx, "Error executing query", zap.String("message", err.Error()))
	}
	return backups
}
