package ydbcp_db_connector

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"go.uber.org/zap"
	"ydbcp/internal/config"
	"ydbcp/internal/types"

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
	SelectRunningBackups() chan types.Backup
}

type YdbDriverImpl struct {
	tableClient table.Client
}

func MakeYdbDriver(config config.Config) *YdbDriverImpl {
	p := new(YdbDriverImpl)
	p.tableClient = InitTableClient(config.YdbcpDbConnectionString)
	return p
}

func InitTableClient(dsn string) table.Client {
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
	// driver must be closed when done
	defer func(db *ydb.Driver, ctx context.Context) {
		err := db.Close(ctx)
		if err != nil {
			xlog.Error(ctx, "Error closing YDB driver")
		}
	}(db, ctx)
	return db.Table()
}

func (d YdbDriverImpl) SelectRunningBackups() chan types.Backup {
	ctx := context.Background()
	backups := make(chan types.Backup)
	err := d.tableClient.Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		var (
			res          result.Result
			backup_id    uuid.UUID
			operation_id uint64
		)
		_, res, err = s.Execute(
			ctx,
			readTx,
			`
			SELECT
				backup_id,
		 		operation_id,
			FROM
				Backups
			WHERE
				status = "PENDING";
      		`, table.NewQueryParameters())
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
			return errors.New("Expected 1 result set")
		}

		for res.NextResultSet(ctx) {
			for res.NextRow() {
				// use ScanNamed to pass column names from the scan string,
				// addresses (and data types) to be assigned the query results
				err = res.ScanNamed(
					named.Optional("backup_id", &backup_id),
					named.Optional("operation_id", &operation_id),
				)
				if err != nil {
					return err
				}
				backups <- types.Backup{Backup_id: backup_id, Operation_id: operation_id}
			}
		}
		return res.Err()
	})
	if err != nil {
		xlog.Error(ctx, "Error executing query", zap.String("message", err.Error()))
	}
	return backups
}
