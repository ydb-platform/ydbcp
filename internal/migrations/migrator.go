package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"

	"github.com/pressly/goose/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"go.uber.org/zap"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
)

// RequiredTables lists YDB tables that must exist in a provisioned ydbcp database.
var RequiredTables = map[string]struct{}{
	"Backups":         {},
	"Operations":      {},
	"OperationTypes":  {},
	"BackupSchedules": {},
}

func HasRequiredTables(ctx context.Context, driver *ydb.Driver) (bool, error) {
	for table := range RequiredTables {
		exists, err := TableExists(ctx, driver, table)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

func TableExists(ctx context.Context, driver *ydb.Driver, table string) (bool, error) {
	tablePath := path.Join(driver.Scheme().Database(), table)
	return sugar.IsTableExists(ctx, driver.Scheme(), tablePath)
}

func OpenDB(ctx context.Context, cfg config.YDBConnectionConfig) (*ydb.Driver, *sql.DB, func(context.Context) error, error) {
	opts, err := db.YdbOptionsFromConfig(cfg, false)
	if err != nil {
		return nil, nil, nil, err
	}

	xlog.Info(ctx, "connecting to ydb for migrations", zap.String(log_keys.ClientDSN, cfg.ConnectionString))
	driver, err := ydb.Open(ctx, cfg.ConnectionString, opts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("can't connect to YDB, dsn %s: %w", cfg.ConnectionString, err)
	}

	connector, err := ydb.Connector(driver,
		ydb.WithDefaultQueryMode(ydb.ScriptingQueryMode),
		ydb.WithFakeTx(ydb.ScriptingQueryMode),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
	)
	if err != nil {
		_ = driver.Close(ctx)
		return nil, nil, nil, fmt.Errorf("failed to create YDB SQL connector: %w", err)
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
	return driver, sqlDB, cleanup, nil
}

func ShouldSkipMigrations(ctx context.Context, driver *ydb.Driver) (bool, error) {
	hasGooseTable, err := TableExists(ctx, driver, goose.DefaultTablename)
	if err != nil {
		return false, err
	}
	if hasGooseTable {
		return false, nil
	}

	hasRequiredTables, err := HasRequiredTables(ctx, driver)
	if err != nil {
		return false, err
	}
	return hasRequiredTables, nil
}

func Run(ctx context.Context, dbConfig config.YDBConnectionConfig, migrationsDir string) error {
	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	if _, err := os.Stat(migrationsDir); err != nil {
		return fmt.Errorf("migrations directory %q: %w", migrationsDir, err)
	}

	driver, sqlDB, cleanup, err := OpenDB(ctx, dbConfig)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := cleanup(ctx); closeErr != nil {
			xlog.Error(ctx, "failed to close migration database connection", zap.Error(closeErr))
		}
	}()

	skip, err := ShouldSkipMigrations(ctx, driver)
	if err != nil {
		return fmt.Errorf("migration pre-checks failed: %w", err)
	}
	if skip {
		xlog.Info(ctx, "Migrations are not needed for this database")
		return nil
	}

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
