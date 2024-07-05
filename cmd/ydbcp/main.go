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
	configInit "ydbcp/internal/config"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ydbcp_db_connector "ydbcp/internal/ydbcp-db-connector"

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
	driver ydbcp_db_connector.YdbDriver
}

// Get implements BackupService
func (s *server) Get(ctx context.Context, in *pb.GetBackupRequest) (*pb.Backup, error) {
	log.Printf("Received: %v", in.GetId())
	backups, err := s.driver.SelectBackups(ctx, types.StatePending)
	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return nil, err
	}
	for _, backup := range backups {
		fmt.Println("backup:", backup.Id.String(), backup.OperationId.String())
	}
	return &pb.Backup{Id: in.GetId()}, nil
}

func main() {
	var confPath string

	flag.StringVar(&confPath, "config", "cmd/ydbcp/config.yaml", "configuration file")
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
		xlog.Debug(ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr))
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	reflection.Register(s)

	ydbServer := server{driver: ydbcp_db_connector.NewYdbDriver(config)}
	defer ydbServer.driver.Close()

	pb.RegisterBackupServiceServer(s, &ydbServer)

	wg.Add(1)
	go func() {
		defer wg.Done()

		xlog.Info(ctx, "server listening", zap.String("address", lis.Addr().String()))
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
