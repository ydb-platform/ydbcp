package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"ydbcp/internal/connectors/s3"

	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"

	"ydbcp/internal/auth"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/handlers"
	"ydbcp/internal/processor"
	"ydbcp/internal/server"
	"ydbcp/internal/server/services/backup"
	"ydbcp/internal/server/services/operation"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"
)

func main() {
	var confPath string

	flag.StringVar(
		&confPath, "config", "config.yaml", "configuration file",
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	logger := xlog.SetupLogging(true)
	xlog.SetInternalLogger(logger)
	defer func() {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("Failed to sync logger: %s\n", err)
		}
	}()

	_, err := maxprocs.Set(maxprocs.Logger(func(f string, p ...interface{}) { xlog.Info(ctx, fmt.Sprintf(f, p...)) }))
	if err != nil {
		xlog.Error(ctx, "Can't set maxprocs", zap.Error(err))
	}

	configInstance, err := config.InitConfig(ctx, confPath)

	if err != nil {
		xlog.Error(ctx, "Unable to initialize config", zap.Error(err))
		os.Exit(1)
	}
	if confStr, err := configInstance.ToString(); err == nil {
		xlog.Debug(
			ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr),
		)
	}
	server, err := server.NewServer(&configInstance.GRPCServer)
	if err != nil {
		xlog.Error(ctx, "failed to initialize GRPC server", zap.Error(err))
		os.Exit(1)
	}

	dbConnector, err := db.NewYdbConnector(ctx, configInstance.DBConnection)
	if err != nil {
		xlog.Error(ctx, "Error init DBConnector", zap.Error(err))
		os.Exit(1)
	}
	defer dbConnector.Close(ctx)
	clientConnector := client.NewClientYdbConnector(configInstance.ClientConnection)
	s3Connector, err := s3.NewS3Connector(configInstance.S3)
	if err != nil {
		xlog.Error(ctx, "Error init S3Connector", zap.Error(err))
		os.Exit(1)
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

	backup.NewBackupService(
		dbConnector,
		clientConnector,
		configInstance.S3,
		authProvider,
		configInstance.ClientConnection.AllowedEndpointDomains,
		configInstance.ClientConnection.AllowInsecureEndpoint,
	).Register(server)
	operation.NewOperationService(dbConnector, authProvider).Register(server)

	if err := server.Start(ctx, &wg); err != nil {
		xlog.Error(ctx, "Error start GRPC server", zap.Error(err))
		os.Exit(1)
	}

	handlersRegistry := processor.NewOperationHandlerRegistry()
	if err := handlersRegistry.Add(
		types.OperationTypeTB,
		handlers.NewTBOperationHandler(dbConnector, clientConnector, s3Connector, configInstance, queries.NewWriteTableQuery),
	); err != nil {
		xlog.Error(ctx, "failed to register TB handler", zap.Error(err))
		os.Exit(1)
	}

	if err := handlersRegistry.Add(
		types.OperationTypeRB,
		handlers.NewRBOperationHandler(dbConnector, clientConnector, configInstance),
	); err != nil {
		xlog.Error(ctx, "failed to register RB handler", zap.Error(err))
		os.Exit(1)
	}

	if err := handlersRegistry.Add(
		types.OperationTypeDB,
		handlers.NewDBOperationHandler(dbConnector, s3Connector, configInstance, queries.NewWriteTableQuery(ctx)),
	); err != nil {
		xlog.Error(ctx, "failed to register DB handler", zap.Error(err))
		os.Exit(1)
	}

	processor.NewOperationProcessor(ctx, &wg, dbConnector, handlersRegistry)

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
