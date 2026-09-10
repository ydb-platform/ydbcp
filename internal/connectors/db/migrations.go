package db

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
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
)

// requiredTables lists YDB tables that must exist in a provisioned ydbcp database.
var requiredTables = map[string]struct{}{
	"Backups":         {},
	"Operations":      {},
	"OperationTypes":  {},
	"BackupSchedules": {},
}

func hasRequiredTables(ctx context.Context, driver *ydb.Driver) (bool, error) {
	for table := range requiredTables {
		exists, err := tableExists(ctx, driver, table)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

func tableExists(ctx context.Context, driver *ydb.Driver, table string) (bool, error) {
	tablePath := path.Join(driver.Scheme().Database(), table)
	return sugar.IsTableExists(ctx, driver.Scheme(), tablePath)
}

func openMigrationDB(ctx context.Context, cfg config.YDBConnectionConfig) (*ydb.Driver, *sql.DB, func(context.Context) error, error) {
	opts, err := ydbOptionsFromConfig(cfg, false)
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

func shouldSkipMigrations(ctx context.Context, driver *ydb.Driver) (bool, error) {
	hasGooseTable, err := tableExists(ctx, driver, goose.DefaultTablename)
	if err != nil {
		return false, err
	}
	if hasGooseTable {
		return false, nil
	}

	hasRequiredTables, err := hasRequiredTables(ctx, driver)
	if err != nil {
		return false, err
	}
	return hasRequiredTables, nil
}

func RunMigrations(ctx context.Context, dbConfig config.YDBConnectionConfig, migrationsDir string) error {
	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	if _, err := os.Stat(migrationsDir); err != nil {
		return fmt.Errorf("migrations directory %q: %w", migrationsDir, err)
	}

	driver, sqlDB, cleanup, err := openMigrationDB(ctx, dbConfig)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := cleanup(ctx); closeErr != nil {
			xlog.Error(ctx, "failed to close migration database connection", zap.Error(closeErr))
		}
	}()

	skip, err := shouldSkipMigrations(ctx, driver)
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

// MigrationConnection owns the database-specific schema operations used by
// migration tooling. Neither the SDK driver nor the SQL connection is exposed.
type MigrationConnection struct {
	driver  *ydb.Driver
	sqlDB   *sql.DB
	cleanup func(context.Context) error
}

func OpenMigrationConnection(ctx context.Context, cfg config.YDBConnectionConfig) (*MigrationConnection, error) {
	driver, sqlDB, cleanup, err := openMigrationDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &MigrationConnection{driver: driver, sqlDB: sqlDB, cleanup: cleanup}, nil
}
func (c *MigrationConnection) Close(ctx context.Context) error { return c.cleanup(ctx) }
func (c *MigrationConnection) HasRequiredTables(ctx context.Context) (bool, error) {
	return hasRequiredTables(ctx, c.driver)
}
func (c *MigrationConnection) TableExists(ctx context.Context, name string) (bool, error) {
	return tableExists(ctx, c.driver, name)
}
func (c *MigrationConnection) ShouldSkipMigrations(ctx context.Context) (bool, error) {
	return shouldSkipMigrations(ctx, c.driver)
}

// MetadataTableNames returns a copy for schema inspection tools.
func MetadataTableNames() []string {
	return []string{"Backups", "Operations", "OperationTypes", "BackupSchedules"}
}

// MigrationHistoryTableName hides the migration library's tracking table name.
func MigrationHistoryTableName() string { return goose.DefaultTablename }

// DropTable is intended for explicit schema maintenance and integration setup.
// Only tables owned by this connector can be dropped.
func (c *MigrationConnection) DropTable(ctx context.Context, name string) error {
	if _, ok := requiredTables[name]; !ok && name != goose.DefaultTablename {
		return fmt.Errorf("unknown metadata table %q", name)
	}
	exists, err := c.TableExists(ctx, name)
	if err != nil || !exists {
		return err
	}
	_, err = c.sqlDB.ExecContext(ctx, "DROP TABLE "+name)
	return err
}
