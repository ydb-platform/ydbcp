package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"ydbcp/internal/audit"
	"ydbcp/internal/auth"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/connectors/s3"
	"ydbcp/internal/handlers"
	"ydbcp/internal/metrics"
	"ydbcp/internal/processor"
	"ydbcp/internal/server"
	"ydbcp/internal/server/services/backup"
	"ydbcp/internal/server/services/backup_schedule"
	"ydbcp/internal/server/services/operation"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	"ydbcp/internal/watchers"
	"ydbcp/internal/watchers/healthcheck"
	"ydbcp/internal/watchers/schedule_watcher"
	"ydbcp/internal/watchers/ttl_watcher"
	ap "ydbcp/pkg/plugins/auth"

	"github.com/jonboulle/clockwork"

	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

func main() {
	var confPath string

	flag.StringVar(
		&confPath, "config", "config.yaml", "configuration file",
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configInstance, err := config.InitConfig[config.Config](ctx, confPath)

	if err != nil {
		log.Error(fmt.Errorf("unable to initialize config: %w", err))
		os.Exit(1)
	}

	var wg sync.WaitGroup

	logger, err := xlog.SetupLogging(configInstance.GRPCServer.LogLevel)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	xlog.SetInternalLogger(logger)
	defer func() {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("Failed to sync logger: %s\n", err)
		}
	}()

	_, err = maxprocs.Set(maxprocs.Logger(func(f string, p ...interface{}) { xlog.Info(ctx, fmt.Sprintf(f, p...)) }))
	if err != nil {
		xlog.Error(ctx, "Can't set maxprocs", zap.Error(err))
	}

	go func() {
		xlog.Info(ctx, "Starting pprof server at :6060")
		err := http.ListenAndServe("localhost:6060", nil)
		if err != nil {
			xlog.Error(ctx, "Failed to start pprof server", zap.Error(err))
		}
	}()

	if confStr, err := configInstance.ToString(); err == nil {
		xlog.Debug(
			ctx, "Use configuration file",
			zap.String("ConfigPath", confPath),
			zap.String("config", confStr),
		)
	}
	audit.EventsDestination = configInstance.AuditEventsDestination
	metrics.InitializeMetricsRegistry(ctx, &wg, &configInstance.MetricsServer, clockwork.NewRealClock())
	xlog.Info(ctx, "Initialized metrics registry")
	server, err := server.NewServer(&configInstance.GRPCServer)
	if err != nil {
		xlog.Error(ctx, "failed to initialize GRPC server", zap.Error(err))
		os.Exit(1)
	}
	xlog.Info(ctx, "created GRPC server")

	dbConnector, err := db.NewYdbConnector(ctx, configInstance.DBConnection)
	if err != nil {
		xlog.Error(ctx, "Error init DBConnector", zap.Error(err))
		os.Exit(1)
	}
	xlog.Info(ctx, "connected to YDBCP database")

	defer dbConnector.Close(ctx)
	clientConnector := client.NewClientYdbConnector(configInstance.ClientConnection)
	s3Connector, err := s3.NewS3Connector(configInstance.S3)
	if err != nil {
		xlog.Error(ctx, "Error init S3Connector", zap.Error(err))
		os.Exit(1)
	}
	xlog.Info(ctx, "connected to YDBCP S3 storage")
	var authProvider ap.AuthProvider
	if len(configInstance.Auth.PluginPath) == 0 {
		authProvider, err = auth.NewDummyAuthProvider(ctx)
	} else {
		authProvider, err = auth.NewAuthProvider(ctx, configInstance.Auth)
	}
	if err != nil {
		xlog.Error(ctx, "Error init AuthProvider", zap.Error(err))
		os.Exit(1)
	}
	defer func() {
		if err := authProvider.Finish(ctx); err != nil {
			xlog.Error(ctx, "Error finish auth provider", zap.Error(err))
		}
	}()
	xlog.Info(ctx, "Initialized AuthProvider")

	backup.NewBackupService(
		dbConnector,
		clientConnector,
		configInstance.S3,
		authProvider,
		configInstance.ClientConnection.AllowedEndpointDomains,
		configInstance.ClientConnection.AllowInsecureEndpoint,
	).Register(server)
	operation.NewOperationService(dbConnector, authProvider).Register(server)
	backup_schedule.NewBackupScheduleService(dbConnector, clientConnector, authProvider, *configInstance).Register(server)
	if err := server.Start(ctx, &wg); err != nil {
		xlog.Error(ctx, "Error start GRPC server", zap.Error(err))
		os.Exit(1)
	}

	xlog.Info(ctx, "Registered services and started GRPC server")

	handlersRegistry := processor.NewOperationHandlerRegistry()
	if err := handlersRegistry.Add(
		types.OperationTypeTB,
		handlers.NewTBOperationHandler(
			dbConnector, clientConnector, s3Connector, *configInstance, queries.NewWriteTableQuery,
		),
	); err != nil {
		xlog.Error(ctx, "failed to register TB handler", zap.Error(err))
		os.Exit(1)
	}

	if err := handlersRegistry.Add(
		types.OperationTypeRB,
		handlers.NewRBOperationHandler(dbConnector, clientConnector, *configInstance),
	); err != nil {
		xlog.Error(ctx, "failed to register RB handler", zap.Error(err))
		os.Exit(1)
	}

	if err := handlersRegistry.Add(
		types.OperationTypeDB,
		handlers.NewDBOperationHandler(dbConnector, s3Connector, *configInstance, queries.NewWriteTableQuery),
	); err != nil {
		xlog.Error(ctx, "failed to register DB handler", zap.Error(err))
		os.Exit(1)
	}

	if err := handlersRegistry.Add(
		types.OperationTypeTBWR,
		handlers.NewTBWROperationHandler(
			dbConnector,
			clientConnector,
			configInstance.S3,
			configInstance.ClientConnection,
			queries.NewWriteTableQuery,
			clockwork.NewRealClock(),
		),
	); err != nil {
		xlog.Error(ctx, "failed to register TBWR handler", zap.Error(err))
		os.Exit(1)
	}

	processor.NewOperationProcessor(ctx, &wg, configInstance.ProcessorIntervalSeconds, dbConnector, handlersRegistry)
	xlog.Info(ctx, "Initialized OperationProcessor")

	if configInstance.DisableTTLDeletion {
		xlog.Info(ctx, "TtlWatcher is disabled, old backups won't be deleted")
	} else {
		ttl_watcher.NewTtlWatcher(ctx, &wg, dbConnector, queries.NewWriteTableQuery)
		xlog.Info(ctx, "Created TtlWatcher")
	}

	backupScheduleHandler := handlers.NewBackupScheduleHandler(queries.NewWriteTableQuery, clockwork.NewRealClock())

	schedule_watcher.NewScheduleWatcher(ctx, &wg, dbConnector, backupScheduleHandler, clockwork.NewRealClock())
	xlog.Info(ctx, "Created ScheduleWatcher")

	healthcheck.NewHealthCheck(ctx, &wg, watchers.WithDisableLog())
	xlog.Info(ctx, "Initialized Healthcheck")

	xlog.Info(ctx, "YDBCP started")
	wg.Add(1)
	go func() {
		defer wg.Done()
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-ctx.Done():
			return
		case sig := <-sigs:
			xlog.Info(ctx, "got signal", zap.String("signal", sig.String()))
			cancel()
		}
	}()
	<-ctx.Done()
	wg.Wait()
}
