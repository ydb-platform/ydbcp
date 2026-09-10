package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.uber.org/zap"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db/internal/queries"
	"ydbcp/internal/metrics"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
)

var (
	readTx              = query.TxControl(query.BeginTx(query.WithOnlineReadOnly()), query.CommitTx())
	writeTx             = query.TxControl(query.BeginTx(query.WithSerializableReadWrite()), query.CommitTx())
	_       DBConnector = (*YdbConnector)(nil)
)

// queryClient is the private SDK seam used by connector tests.
type queryClient interface {
	Do(context.Context, query.Operation, ...query.DoOption) error
}

type YdbConnector struct {
	driver *ydb.Driver
	client queryClient
}

func select1(baseCtx context.Context, driver *ydb.Driver, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()
	return driver.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, res, err := s.Execute(ctx, table.TxControl(
			table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx(),
		), "SELECT 1", nil)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, res.Close()) }()
		if res.ResultSetCount() != 1 {
			return errors.New("expected 1 result set")
		}
		return res.Err()
	})
}

func NewYdbConnector(ctx context.Context, cfg config.YDBConnectionConfig) (*YdbConnector, error) {
	opts, err := ydbOptionsFromConfig(cfg, cfg.EnableSDKMetrics)
	if err != nil {
		return nil, err
	}
	xlog.Info(ctx, "connecting to ydb", zap.String(log_keys.ClientDSN, cfg.ConnectionString))
	driver, err := ydb.Open(ctx, cfg.ConnectionString, opts...)
	if err != nil {
		return nil, fmt.Errorf("can't connect to YDB, dsn %s: %w", cfg.ConnectionString, err)
	}
	if err = select1(ctx, driver, time.Second*time.Duration(cfg.DialTimeoutSeconds)); err != nil {
		_ = driver.Close(ctx)
		return nil, fmt.Errorf("can't connect to YDB, dsn %s: %w", cfg.ConnectionString, err)
	}
	return &YdbConnector{driver: driver, client: driver.Query()}, nil
}

// Close belongs to connection ownership in main, not to the persistence contract.
func (d *YdbConnector) Close(ctx context.Context) {
	if err := d.driver.Close(ctx); err != nil {
		xlog.Error(ctx, "Error closing YDB driver", zap.Error(err))
	}
}

func selectRows[T any](ctx context.Context, d *YdbConnector, builder queries.ReadTableQuery, decode func(query.Row) (T, error)) ([]T, error) {
	q, err := builder.FormatQuery(ctx)
	if err != nil {
		return nil, err
	}
	var entities []T
	err = d.client.Do(ctx, func(ctx context.Context, s query.Session) (err error) {
		// The SDK can retry the entire callback after a partially consumed result.
		entities = nil
		res, err := s.Query(ctx, q.QueryText, query.WithParameters(q.QueryParams), query.WithTxControl(readTx))
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, res.Close(ctx)) }()
		sets := 0
		for {
			set, err := res.NextResultSet(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			sets++
			if sets > 1 {
				return errors.New("expected 1 result set")
			}
			for {
				row, err := set.NextRow(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				entity, err := decode(row)
				if err != nil {
					return fmt.Errorf("decode metadata row: %w", err)
				}
				entities = append(entities, entity)
			}
		}
		return nil
	})
	if err != nil {
		reportDBError(ctx, err)
		return nil, err
	}
	return entities, nil
}

func reportDBError(ctx context.Context, err error) {
	xlog.Error(ctx, "Error executing metadata query", zap.Error(err))
	metrics.GlobalMetricsRegistry.IncYdbErrorsCounter()
}
