package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/pressly/goose/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
)

func openSQLDB(ctx context.Context, cfg config.YDBConnectionConfig) (*sql.DB, func(context.Context) error, error) {
	opts, err := db.YdbOptionsFromConfig(cfg, false)
	if err != nil {
		return nil, nil, err
	}

	xlog.Info(ctx, "connecting to ydb for migrations", zap.String(log_keys.ClientDSN, cfg.ConnectionString))
	driver, err := ydb.Open(ctx, cfg.ConnectionString, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("can't connect to YDB, dsn %s: %w", cfg.ConnectionString, err)
	}

	connector, err := ydb.Connector(driver,
		ydb.WithDefaultQueryMode(ydb.ScriptingQueryMode),
		ydb.WithFakeTx(ydb.ScriptingQueryMode),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
	)
	if err != nil {
		_ = driver.Close(ctx)
		return nil, nil, fmt.Errorf("failed to create YDB SQL connector: %w", err)
	}

	sqlDB := sql.OpenDB(connector)
	cleanup := func(ctx context.Context) error {
		if err := sqlDB.Close(); err != nil {
			return fmt.Errorf("failed to close SQL connection: %w", err)
		}
		if err := driver.Close(ctx); err != nil {
			return fmt.Errorf("failed to close YDB driver: %w", err)
		}
		return nil
	}
	return sqlDB, cleanup, nil
}

func Run(ctx context.Context, dbConfig config.YDBConnectionConfig, migrationsDir string) error {
	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	if _, err := os.Stat(migrationsDir); err != nil {
		return fmt.Errorf("migrations directory %q: %w", migrationsDir, err)
	}

	sqlDB, cleanup, err := openSQLDB(ctx, dbConfig)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := cleanup(ctx); closeErr != nil {
			xlog.Error(ctx, "failed to close migration database connection", zap.Error(closeErr))
		}
	}()

	provider, err := goose.NewProvider(goose.DialectYdB, sqlDB, os.DirFS(migrationsDir))
	if err != nil {
		return fmt.Errorf("failed to create goose provider: %w", err)
	}

	xlog.Info(ctx, "running database migrations", zap.String("migrations_dir", migrationsDir))
	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	for _, result := range results {
		xlog.Info(ctx, "applied migration",
			zap.Int64("version", result.Source.Version),
			zap.Duration("duration", result.Duration),
		)
	}
	if len(results) == 0 {
		xlog.Info(ctx, "database is up to date")
	}
	return nil
}
