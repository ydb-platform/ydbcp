package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
)

const (
	defaultConnectionString = "grpcs://local-ydb:2135/local"
	defaultMigrationsDir    = "migrations/yql"
)

type testEnv struct {
	ctx           context.Context
	cfg           config.YDBConnectionConfig
	migrationsDir string
	conn          *db.MigrationConnection
}

func main() {
	ctx := context.Background()
	cfg := dbConfig()

	conn, err := db.OpenMigrationConnection(ctx, cfg)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Fatalf("close database: %v", err)
		}
	}()

	env := testEnv{
		ctx:           ctx,
		cfg:           cfg,
		migrationsDir: migrationsDir(),
		conn:          conn,
	}

	if err := runOnDBWithoutGooseTracking(env); err != nil {
		log.Fatalf("run on db without goose tracking: %v", err)
	}
	if err := runOnEmptyDB(env); err != nil {
		log.Fatalf("run on empty db: %v", err)
	}
	if err := runOnMigratedDB(env); err != nil {
		log.Fatalf("run on migrated db: %v", err)
	}

	log.Println("migrate integration test passed")
}

// runOnMigratedDB checks that init_ydb_schema left a migrated database and Run is idempotent.
func runOnMigratedDB(env testEnv) error {
	log.Println("case: run migrations on migrated db")

	if err := checkRequiredTables(env.ctx, env.conn); err != nil {
		return fmt.Errorf("precondition: %w", err)
	}
	if err := db.RunMigrations(env.ctx, env.cfg, env.migrationsDir); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}

// runOnEmptyDB checks that migrations can bootstrap a database from scratch.
func runOnEmptyDB(env testEnv) error {
	log.Println("case: run migrations on empty db")

	if err := dropAllTables(env.ctx, env.conn); err != nil {
		return fmt.Errorf("prepare empty db: %w", err)
	}
	if err := checkNoRequiredTables(env.ctx, env.conn); err != nil {
		return fmt.Errorf("precondition: %w", err)
	}
	if err := checkShouldSkipThenRun(env.ctx, env.cfg, env.migrationsDir /* should skip */, false); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	if err := checkRequiredTables(env.ctx, env.conn); err != nil {
		return fmt.Errorf("postcondition: %w", err)
	}
	if err := checkGooseTable(env.ctx, env.conn, true); err != nil {
		return fmt.Errorf("postcondition: %w", err)
	}
	return nil
}

// runOnDBWithoutGooseTracking checks that databases without goose tracking are left unchanged.
func runOnDBWithoutGooseTracking(env testEnv) error {
	log.Println("case: run migrations on db without goose tracking")

	if err := dropTable(env.ctx, env.conn, db.MigrationHistoryTableName()); err != nil {
		return fmt.Errorf("prepare db without goose tracking: %w", err)
	}
	if err := checkRequiredTables(env.ctx, env.conn); err != nil {
		return fmt.Errorf("precondition: %w", err)
	}
	if err := checkShouldSkipThenRun(env.ctx, env.cfg, env.migrationsDir /* should skip */, true); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	if err := checkGooseTable(env.ctx, env.conn, false); err != nil {
		return fmt.Errorf("postcondition: %w", err)
	}
	return nil
}

func dbConfig() config.YDBConnectionConfig {
	dsn := os.Getenv("YDB_CONNECTION_STRING")
	if dsn == "" {
		dsn = defaultConnectionString
	}
	return config.YDBConnectionConfig{
		ConnectionString: dsn,
		Insecure:         true,
		Discovery:        false,
	}
}

func migrationsDir() string {
	if dir := os.Getenv("MIGRATIONS_DIR"); dir != "" {
		return dir
	}
	return defaultMigrationsDir
}

func checkRequiredTables(ctx context.Context, conn *db.MigrationConnection) error {
	hasRequired, err := conn.HasRequiredTables(ctx)
	if err != nil {
		return err
	}
	if !hasRequired {
		return fmt.Errorf("required tables must exist")
	}
	return nil
}

func checkNoRequiredTables(ctx context.Context, conn *db.MigrationConnection) error {
	hasRequired, err := conn.HasRequiredTables(ctx)
	if err != nil {
		return err
	}
	if hasRequired {
		return fmt.Errorf("required tables must not exist")
	}
	return nil
}

func checkGooseTable(ctx context.Context, conn *db.MigrationConnection, want bool) error {
	hasGooseTable, err := conn.TableExists(ctx, db.MigrationHistoryTableName())
	if err != nil {
		return err
	}
	if hasGooseTable != want {
		if want {
			return fmt.Errorf("goose version table must exist")
		}
		return fmt.Errorf("goose version table must not exist")
	}
	return nil
}

func checkShouldSkipThenRun(ctx context.Context, cfg config.YDBConnectionConfig, migrationsDir string, wantSkip bool) error {
	conn, err := db.OpenMigrationConnection(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	skip, err := conn.ShouldSkipMigrations(ctx)
	if err != nil {
		return err
	}
	if skip != wantSkip {
		if wantSkip {
			return fmt.Errorf("expected migrations to be skipped")
		}
		return fmt.Errorf("expected migrations to run")
	}

	if err := db.RunMigrations(ctx, cfg, migrationsDir); err != nil {
		return err
	}
	return nil
}

func dropAllTables(ctx context.Context, conn *db.MigrationConnection) error {
	for _, table := range db.MetadataTableNames() {
		if err := dropTable(ctx, conn, table); err != nil {
			return err
		}
	}
	return dropTable(ctx, conn, db.MigrationHistoryTableName())
}

func dropTable(ctx context.Context, conn *db.MigrationConnection, table string) error {
	exists, err := conn.TableExists(ctx, table)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := conn.DropTable(ctx, table); err != nil {
		return fmt.Errorf("drop table %s: %w", table, err)
	}
	return nil
}
