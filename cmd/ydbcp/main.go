package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"ydbcp/internal/config"
	configInit "ydbcp/internal/config"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"

	pb "ydbcp/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement BackupService.
type server struct {
	pb.UnimplementedBackupServiceServer
	driver db.DBConnector
	s3     config.S3Config
}

// GetBackup implements BackupService
func (s *server) GetBackup(ctx context.Context, in *pb.GetBackupRequest) (*pb.Backup, error) {
	log.Printf("Received: %v", in.GetId())
	backups, err := s.driver.SelectBackups(ctx, types.BackupStatePending)
	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return nil, err
	}
	for _, backup := range backups {
		fmt.Println("backup:", backup.ID.String())
	}
	return &pb.Backup{Id: in.GetId()}, nil
}

func (s *server) MakeBackup(ctx context.Context, req *pb.MakeBackupRequest) (*pb.Operation, error) {
	xlog.Info(ctx, "MakeBackup called", zap.String("request", req.String()))

	backup := types.Backup{
		ContainerID:  req.GetContainerId(),
		DatabaseName: req.GetDatabaseName(),
		S3Endpoint:   s.s3.Endpoint,
		S3Region:     s.s3.Region,
		S3Bucket:     s.s3.Bucket,
		S3PathPrefix: s.s3.PathPrefix,
		Status:       types.BackupStatePending,
	}
	backupID, err := s.driver.CreateBackup(ctx, backup)
	if err != nil {
		xlog.Error(
			ctx, "can't create backup",
			zap.String("backup", backup.String()),
			zap.Error(err),
		)
		return nil, err
	}

	op := types.NewTakeBackupOperation()
	op.YdbConnectionParams = types.YdbConnectionParams{
		Endpoint:     req.GetEndpoint(),
		DatabaseName: req.GetDatabaseName(),
	}
	op.SourcePaths = req.GetSourcePaths()
	op.SourcePathToExclude = req.GetSourcePathsToExclude()
	op.BackupId = backupID

	operationID, err := s.driver.CreateOperation(ctx, op)
	if err != nil {
		xlog.Error(ctx, "can't create operation", zap.String("operation", types.OperationToString(op)), zap.Error(err))
		return nil, err
	}

	// TODO: get pb.Operation from Operation in one place
	return &pb.Operation{
		Id:                   operationID.String(),
		ContainerId:          req.ContainerId,
		Type:                 string(op.GetType()),
		DatabaseName:         op.YdbConnectionParams.DatabaseName,
		BackupId:             backupID.String(),
		SourcePaths:          op.SourcePaths,
		SourcePathsToExclude: op.SourcePathToExclude,
		Status:               types.OperationState(op.GetState()).Enum(),
	}, nil
}

func main() {
	var confPath string

	flag.StringVar(
		&confPath, "config", "cmd/ydbcp/config.yaml", "configuration file",
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	logger := xlog.SetupLogging(true)
	xlog.SetInternalLogger(logger)
	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("Failed to sync logger: %s\n", err)
		}
	}(logger)

	config, err := configInit.InitConfig(ctx, confPath)

	if err != nil {
		xlog.Error(ctx, "Unable to initialize config", zap.Error(err))
		os.Exit(1)
	}
	confStr, err := config.ToString()
	if err == nil {
		xlog.Debug(
			ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr),
		)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	reflection.Register(s)

	ydbServer := server{driver: db.NewYdbConnector(config)}
	defer ydbServer.driver.Close()

	pb.RegisterBackupServiceServer(s, &ydbServer)

	wg.Add(1)
	go func() {
		defer wg.Done()

		xlog.Info(
			ctx, "server listening", zap.String("address", lis.Addr().String()),
		)
		if err := s.Serve(lis); err != nil {
			xlog.Error(ctx, "failed to serve", zap.Error(err))
		}
	}()

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
	s.GracefulStop()
	wg.Wait()
}
