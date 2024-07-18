package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"ydbcp/internal/config"
	configInit "ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/handlers"
	"ydbcp/internal/processor"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"

	pb "ydbcp/pkg/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Export"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement BackupService.
type server struct {
	pb.UnimplementedBackupServiceServer
	driver     db.DBConnector
	clientConn client.ClientConnector
	s3         *config.S3Config
}

// GetBackup implements BackupService
func (s *server) GetBackup(ctx context.Context, in *pb.GetBackupRequest) (*pb.Backup, error) {
	log.Printf("Received: %v", in.GetId())
	backups, err := s.driver.SelectBackupsByStatus(ctx, types.BackupStatePending)
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

	clientConnectionParams := types.YdbConnectionParams{
		Endpoint:     req.GetEndpoint(),
		DatabaseName: req.GetDatabaseName(),
	}
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	client, err := s.clientConn.Open(ctx, dsn)
	if err != nil {
		xlog.Error(ctx, "can't open client connection", zap.Error(err), zap.String("dsn", dsn))
		return nil, fmt.Errorf("can't open client connection, dsn %s: %w", dsn, err)
	}
	defer func() {
		if err := s.clientConn.Close(ctx, client); err != nil {
			xlog.Error(ctx, "can't close client connection", zap.Error(err))
		}
	}()

	accessKey, err := s.s3.AccessKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3AccessKey", zap.Error(err))
		return nil, fmt.Errorf("can't get S3AccessKey: %w", err)
	}
	secretKey, err := s.s3.SecretKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3SecretKey", zap.Error(err))
		return nil, fmt.Errorf("can't get S3SecretKey: %w", err)
	}
	exportItems := make([]*Ydb_Export.ExportToS3Settings_Item, 0, len(s.s3.PathPrefix))
	databaseName := strings.Replace(req.DatabaseName, "/", "_", -1) // TODO: checking user imput
	dstPrefix := fmt.Sprintf("%s/%s/%s", s.s3.PathPrefix, databaseName, time.Now().Format("20060102_150405"))
	for _, path := range req.GetSourcePaths() {
		exportItems = append(exportItems, &Ydb_Export.ExportToS3Settings_Item{
			SourcePath:        path,
			DestinationPrefix: dstPrefix, // TODO: check if we need different destination prefixes?
		})
	}

	s3Settings := &Ydb_Export.ExportToS3Settings{
		Endpoint:        s.s3.Endpoint,
		Region:          s.s3.Region,
		Bucket:          s.s3.Region,
		AccessKey:       accessKey,
		SecretKey:       secretKey,
		Description:     "ydbcp backup", // TODO: the description shoud be better
		NumberOfRetries: 10,             // TODO: get it from configuration
		Items:           exportItems,
	}

	clientOperationID, err := s.clientConn.ExportToS3(ctx, client, s3Settings)
	if err != nil {
		xlog.Error(ctx, "can't start export operation", zap.Error(err), zap.String("dns", dsn))
		return nil, fmt.Errorf("can't start export operation, dsn %s: %w", dsn, err)
	}
	xlog.Debug(ctx, "export operation started", zap.String("clientOperationID", clientOperationID), zap.String("dsn", dsn))

	backup := types.Backup{
		ContainerID:  req.GetContainerId(),
		DatabaseName: req.GetDatabaseName(),
		S3Endpoint:   s.s3.Endpoint,
		S3Region:     s.s3.Region,
		S3Bucket:     s.s3.Bucket,
		S3PathPrefix: dstPrefix,
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

	op := &types.TakeBackupOperation{
		BackupId: backupID,
		State:    types.OperationStatePending,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.GetEndpoint(),
			DatabaseName: req.GetDatabaseName(),
		},
		YdbOperationId:      clientOperationID,
		SourcePaths:         req.GetSourcePaths(),
		SourcePathToExclude: req.GetSourcePathsToExclude(),
	}

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

func (s *server) ListBackups(ctx context.Context, request *pb.ListBackupsRequest) (*pb.ListBackupsResponse, error) {
	log.Printf("ListBackups: %s", request.String())
	backups, err := s.driver.SelectBackups(
		ctx, queries.MakeReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields(queries.AllBackupFields...),
			queries.WithQueryFilters(
				queries.QueryFilter[string]{
					Field:  "container_id",
					Values: []string{request.ContainerId},
				},
				queries.QueryFilter[string]{
					Field:  "database",
					Values: []string{request.DatabaseNameMask},
					IsLike: true,
				},
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting backups: %w", err)
	}
	pbBackups := make([]*pb.Backup, 0, len(backups))
	for _, backup := range backups {
		pbBackups = append(pbBackups, backup.Proto())
	}
	return &pb.ListBackupsResponse{
		Backups:       pbBackups,
		NextPageToken: strconv.Itoa(len(backups)),
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

	config, err := configInit.NewConfig(ctx, confPath)
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
		return
	}
	s := grpc.NewServer()
	reflection.Register(s)

	dbConnector := db.NewYdbConnector(config)

	server := server{
		driver:     dbConnector,
		clientConn: client.NewClientYdbConnector(),
		s3:         &config.S3,
	}
	defer server.driver.Close()

	pb.RegisterBackupServiceServer(s, &server)

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

	handlersRegistry := processor.NewOperationHandlerRegistry()
	err = handlersRegistry.Add(
		types.OperationType("TB"), handlers.MakeTBOperationHandler(dbConnector, client.NewClientYdbConnector()),
	)
	if err != nil {
		log.Fatalf("failed to register handler: %v", err)
		return
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
	s.GracefulStop()
	wg.Wait()
}
