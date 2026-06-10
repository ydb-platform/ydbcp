package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"go.uber.org/zap"

	"ydbcp/internal/config"
	"ydbcp/internal/migrations"
	"ydbcp/internal/util/xlog"
)

func main() {
	var (
		confPath      string
		migrationsDir string
	)

	flag.StringVar(&confPath, "config", "config.yaml", "configuration file")
	flag.StringVar(&migrationsDir, "migrations-dir", "", "directory with SQL migrations (required)")
	flag.Parse()

	if migrationsDir == "" {
		log.Error(fmt.Errorf("migrations-dir is required"))
		os.Exit(1)
	}

	ctx := context.Background()

	cfg, err := config.InitConfig[config.Config](ctx, confPath)
	if err != nil {
		log.Error(fmt.Errorf("unable to initialize config: %w", err))
		os.Exit(1)
	}

	logger, err := xlog.SetupLogging(cfg.Log.Level)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	xlog.SetInternalLogger(logger)
	defer func() {
		if err := logger.Sync(); err != nil {
			fmt.Printf("Failed to sync logger: %s\n", err)
		}
	}()

	if err := migrations.Run(ctx, cfg.DBConnection, migrationsDir); err != nil {
		xlog.Error(ctx, "migration failed", zap.Error(err))
		os.Exit(1)
	}
}
