package main

import (
	"context"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"log"
	"net"
	configInit "ydbcp/internal/config"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	"ydbcp/internal/ydbcp-db-connector"

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
	log.Printf("Received: %v", in.GetBackupId())
	backups := s.driver.SelectBackups(ctx, types.STATUS_PENDING)
	for _, backup := range backups {
		fmt.Println("backup:", backup.Backup_id.String(), *backup.Operation_id)
	}
	return &pb.Backup{BackupId: in.GetBackupId()}, nil
}

func main() {
	var confPath string

	flag.StringVar(&confPath, "config", "cmd/ydbcp/config.yaml", "aardappel configuration file")
	flag.Parse()
	ctx := context.Background()

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

	log.Printf("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
