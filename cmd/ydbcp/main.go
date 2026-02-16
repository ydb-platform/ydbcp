package main

import (
	"context"
	"flag"
	"fmt"
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
	ydbcpCredentials "ydbcp/internal/credentials"
	"ydbcp/internal/handlers"
	"ydbcp/internal/kms"
	"ydbcp/internal/metrics"
	"ydbcp/internal/processor"
	"ydbcp/internal/server"
	"ydbcp/internal/server/services/backup"
	"ydbcp/internal/server/services/backup_schedule"
	"ydbcp/internal/server/services/operation"
	"ydbcp/internal/types"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
	"ydbcp/internal/watchers"
	"ydbcp/internal/watchers/healthcheck"
	"ydbcp/internal/watchers/schedule_watcher"
	"ydbcp/internal/watchers/ttl_watcher"
	ap "ydbcp/pkg/plugins/auth"
	kp "ydbcp/pkg/plugins/kms"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"

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

	var logger *xlog.LogConfig
	if configInstance.Log.DuplicateToFile != "" {
		logger, err = xlog.SetupLoggingWithFile(configInstance.Log.Level, configInstance.Log.DuplicateToFile)
	} else {
		logger, err = xlog.SetupLogging(configInstance.Log.Level)
	}
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
			zap.String(log_keys.ConfigPath, confPath),
			zap.String(log_keys.Config, confStr),
		)
	}

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

	var kmsProvider kp.KmsProvider
	if len(configInstance.KMS.PluginPath) == 0 {
		kmsProvider, err = kms.NewDummyKmsProvider(ctx)
	} else {
		kmsProvider, err = kms.NewKmsProvider(ctx, configInstance.KMS)
	}
	if err != nil {
		xlog.Error(ctx, "Error init KmsProvider", zap.Error(err))
		os.Exit(1)
	}
	defer func() {
		if err := kmsProvider.Close(ctx); err != nil {
			xlog.Error(ctx, "Error close kms provider", zap.Error(err))
		}
	}()
	xlog.Info(ctx, "Initialized KmsProvider")

	metrics.InitializeMetricsRegistry(ctx, &wg, &configInstance.MetricsServer, clockwork.NewRealClock())
	xlog.Info(ctx, "Initialized metrics registry")
	audit.EventsDestination = configInstance.Audit.EventsDestination
	server, err := server.NewServer(&configInstance.GRPCServer, authProvider)
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
	if provider, ok := authProvider.(interface{ SetYDBCPContainerID(string) }); ok {
		if configInstance.DBConnection.K8sJWTAuth == nil {
			xlog.Error(ctx, "inconsistent configuration: db_connection.k8s_jwt_auth is required for YDBCP container discovery")
			os.Exit(1)
		}
		securityToken, tokenErr := ydbcpCredentials.GetK8sJWTToken(ctx, configInstance.DBConnection.K8sJWTAuth)
		if tokenErr != nil {
			xlog.Error(ctx, "failed to get security token for YDBCP container discovery", zap.Error(tokenErr))
			os.Exit(1)
		}
		containerID, discoverErr := auth.DiscoverYDBCPContainerID(
			ctx,
			dbConnector.GRPCConn(),
			configInstance.DBConnection.ConnectionString,
			securityToken,
		)
		if discoverErr != nil {
			xlog.Error(ctx, "failed to resolve YDBCP container ID", zap.Error(discoverErr))
			os.Exit(1)
		}
		provider.SetYDBCPContainerID(containerID)
		xlog.Info(ctx, "resolved YDBCP container ID from YDB", zap.String(log_keys.ContainerID, containerID))
	}

	defer dbConnector.Close(ctx)
	clientConnector := client.NewClientYdbConnector(configInstance.ClientConnection)
	s3Connector, err := s3.NewS3Connector(configInstance.S3)
	if err != nil {
		xlog.Error(ctx, "Error init S3Connector", zap.Error(err))
		os.Exit(1)
	}
	xlog.Info(ctx, "connected to YDBCP S3 storage")

	backup.NewBackupService(
		dbConnector,
		clientConnector,
		s3Connector,
		authProvider,
		kmsProvider,
		*configInstance,
	).Register(server)
	operation.NewOperationService(dbConnector, authProvider).Register(server)
	backup_schedule.NewBackupScheduleService(
		dbConnector, clientConnector, authProvider, *configInstance,
	).Register(server)
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
			s3Connector,
			queries.NewWriteTableQuery,
			clockwork.NewRealClock(),
			*configInstance,
			kmsProvider,
		),
	); err != nil {
		xlog.Error(ctx, "failed to register TBWR handler", zap.Error(err))
		os.Exit(1)
	}

	processor.NewOperationProcessor(
		ctx, &wg, configInstance.OperationProcessor.ProcessorIntervalSeconds, dbConnector, handlersRegistry,
	)
	xlog.Info(ctx, "Initialized OperationProcessor")

	if configInstance.FeatureFlags.DisableTTLDeletion {
		xlog.Info(ctx, "TtlWatcher is disabled, old backups won't be deleted")
	} else {
		ttl_watcher.NewTtlWatcher(ctx, &wg, dbConnector, queries.NewWriteTableQuery)
		xlog.Info(ctx, "Created TtlWatcher")
	}

	backupScheduleHandler := handlers.NewBackupScheduleHandler(queries.NewWriteTableQuery, clockwork.NewRealClock(), configInstance.FeatureFlags)

	schedule_watcher.NewScheduleWatcher(
		ctx, &wg, configInstance.OperationProcessor.ProcessorIntervalSeconds, dbConnector,
		backupScheduleHandler, clockwork.NewRealClock(),
	)
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
			xlog.Info(ctx, "got signal", zap.String(log_keys.Signal, sig.String()))
			cancel()
		}
	}()
	<-ctx.Done()
	wg.Wait()
}
