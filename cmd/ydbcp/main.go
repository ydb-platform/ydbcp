package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"

	"ydbcp/internal/auth"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/handlers"
	"ydbcp/internal/processor"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

var (
	port                = flag.Int("port", 50051, "The server port")
	errPermissionDenied = errors.New("permission denied")
	errGetAuthToken     = errors.New("can't get auth token")
)

// server is used to implement BackupService.
type server struct {
	pb.UnimplementedBackupServiceServer
	pb.UnimplementedOperationServiceServer
	driver     db.DBConnector
	clientConn client.ClientConnector
	s3         config.S3Config
	auth       ap.AuthProvider
}

func tokenFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errGetAuthToken
	}
	tokens, ok := md["authorization"]
	if !ok {
		return "", fmt.Errorf("can't find authorization header, %w", errGetAuthToken)
	}
	if len(tokens) == 0 {
		return "", fmt.Errorf("incorrect authorization header format, %w", errGetAuthToken)
	}
	token := tokens[0]
	if len(token) < 8 || token[0:7] != "Bearer " {
		return "", fmt.Errorf("incorrect authorization header format, %w", errGetAuthToken)
	}
	token = token[7:]
	return token, nil
}

func (s *server) checkAuth(ctx context.Context, permission, containerID, resourceID string) (string, error) {
	token, err := tokenFromContext(ctx)
	if err != nil {
		xlog.Debug(ctx, "can't get auth token", zap.Error(err))
		token = ""
	}
	check := ap.AuthorizeCheck{
		Permission:  permission,
		ContainerID: containerID,
	}
	if len(resourceID) > 0 {
		check.ResourceID = []string{resourceID}
	}

	resp, subject, err := s.auth.Authorize(ctx, token, check)
	if err != nil {
		xlog.Error(ctx, "auth plugin authorize error", zap.Error(err))
		return "", errPermissionDenied
	}
	if len(resp) != 1 {
		xlog.Error(ctx, "incorrect auth plugin response length != 1")
		return "", errPermissionDenied
	}
	if resp[0].Code != ap.AuthCodeSuccess {
		xlog.Error(ctx, "auth plugin response", zap.Int("code", int(resp[0].Code)), zap.String("message", resp[0].Message))
		return "", errPermissionDenied
	}
	return subject, nil
}

func (s *server) GetBackup(ctx context.Context, request *pb.GetBackupRequest) (*pb.Backup, error) {
	xlog.Debug(ctx, "GetBackup", zap.String("request", request.String()))
	requestId, err := types.ParseObjectId(request.GetId())
	if err != nil {
		return nil, fmt.Errorf("failed to parse uuid %s: %w", request.GetId(), err)
	}
	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields(queries.AllBackupFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.UUIDValue(requestId)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return nil, err
	}
	if len(backups) == 0 {
		return nil, errors.New("no backup with such Id") // TODO: Permission denied?
	}
	// TODO: Need to check access to backup resource by backupID
	if _, err := s.checkAuth(ctx, auth.PermissionBackupGet, backups[0].ContainerID, ""); err != nil {
		return nil, err
	}

	xlog.Debug(ctx, "GetBackup", zap.String("backup", backups[0].String()))
	return backups[0].Proto(), nil
}

func (s *server) MakeBackup(ctx context.Context, req *pb.MakeBackupRequest) (*pb.Operation, error) {
	xlog.Info(ctx, "MakeBackup", zap.String("request", req.String()))
	subject, err := s.checkAuth(ctx, auth.PermissionBackupCreate, req.ContainerId, "")
	if err != nil {
		return nil, err
	}
	xlog.Debug(ctx, "MakeBackup", zap.String("subject", subject))

	clientConnectionParams := types.YdbConnectionParams{
		Endpoint:     req.GetDatabaseEndpoint(),
		DatabaseName: req.GetDatabaseName(),
	}
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	client, err := s.clientConn.Open(ctx, dsn)
	if err != nil {
		// xlog.Error(ctx, "can't open client connection", zap.Error(err), zap.String("dsn", dsn))
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

	dbNamePath := strings.Replace(req.DatabaseName, "/", "_", -1) // TODO: checking user input
	dbNamePath = strings.Trim(dbNamePath, "_")
	dstPrefix := path.Join(s.s3.PathPrefix, dbNamePath)

	s3Settings := types.ExportSettings{
		Endpoint:            s.s3.Endpoint,
		Region:              s.s3.Region,
		Bucket:              s.s3.Bucket,
		AccessKey:           accessKey,
		SecretKey:           secretKey,
		Description:         "ydbcp backup", // TODO: the description shoud be better
		NumberOfRetries:     10,             // TODO: get it from configuration
		SourcePaths:         req.GetSourcePaths(),
		SourcePathToExclude: req.GetSourcePathsToExclude(),
		DestinationPrefix:   s.s3.PathPrefix,
		BackupID:            types.GenerateObjectID(), // TODO: do we need backup id?
	}

	clientOperationID, err := s.clientConn.ExportToS3(ctx, client, s3Settings)
	if err != nil {
		xlog.Error(ctx, "can't start export operation", zap.Error(err), zap.String("dns", dsn))
		return nil, fmt.Errorf("can't start export operation, dsn %s: %w", dsn, err)
	}
	xlog.Debug(
		ctx, "export operation started", zap.String("clientOperationID", clientOperationID), zap.String("dsn", dsn),
	)

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
		BackupId:    backupID,
		ContainerID: req.ContainerId,
		State:       types.OperationStatePending,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.GetDatabaseEndpoint(),
			DatabaseName: req.GetDatabaseName(),
		},
		SourcePaths:         req.GetSourcePaths(),
		SourcePathToExclude: req.GetSourcePathsToExclude(),
		CreatedAt:           time.Now(),
		YdbOperationId:      clientOperationID,
	}

	operationID, err := s.driver.CreateOperation(ctx, op)
	if err != nil {
		xlog.Error(ctx, "can't create operation", zap.String("operation", types.OperationToString(op)), zap.Error(err))
		return nil, err
	}

	op.Id = operationID
	return op.Proto(), nil
}

func (s *server) MakeRestore(ctx context.Context, req *pb.MakeRestoreRequest) (*pb.Operation, error) {
	xlog.Info(ctx, "MakeRestore", zap.String("request", req.String()))

	subject, err := s.checkAuth(ctx, auth.PermissionBackupRestore, req.ContainerId, "") // TODO: check access to backup as resource
	if err != nil {
		return nil, err
	}
	xlog.Debug(ctx, "MakeRestore", zap.String("subject", subject))

	clientConnectionParams := types.YdbConnectionParams{
		Endpoint:     req.GetDatabaseEndpoint(),
		DatabaseName: req.GetDatabaseName(),
	}
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	client, err := s.clientConn.Open(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("can't open client connection, dsn %s: %w", dsn, err)
	}
	defer func() {
		if err := s.clientConn.Close(ctx, client); err != nil {
			xlog.Error(ctx, "can't close client connection", zap.Error(err))
		}
	}()

	accessKey, err := s.s3.AccessKey()
	if err != nil {
		return nil, fmt.Errorf("can't get S3AccessKey: %w", err)
	}
	secretKey, err := s.s3.SecretKey()
	if err != nil {
		return nil, fmt.Errorf("can't get S3SecretKey: %w", err)
	}

	s3Settings := types.ImportSettings{
		Endpoint:          s.s3.Endpoint,
		Bucket:            s.s3.Bucket,
		AccessKey:         accessKey,
		SecretKey:         secretKey,
		Description:       "ydbcp restore", // TODO: write description
		NumberOfRetries:   10,              // TODO: get value from configuration
		BackupID:          req.GetBackupId(),
		SourcePaths:       req.GetSourcePaths(),
		S3ForcePathStyle:  true,
		DestinationPrefix: req.GetDestinationPrefix(),
	}

	clientOperationID, err := s.clientConn.ImportFromS3(ctx, client, s3Settings)
	if err != nil {
		return nil, fmt.Errorf("can't start import operation, dsn %s: %w", dsn, err)
	}

	xlog.Debug(
		ctx, "import operation started", zap.String("clientOperationID", clientOperationID), zap.String("dsn", dsn),
	)

	op := &types.RestoreBackupOperation{
		ContainerID: req.GetContainerId(),
		BackupId:    types.MustObjectIDFromString(req.GetBackupId()),
		State:       types.OperationStatePending,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.GetDatabaseEndpoint(),
			DatabaseName: req.GetDatabaseName(),
		},
		YdbOperationId: clientOperationID,
		CreatedAt:      time.Now(),
	}

	operationID, err := s.driver.CreateOperation(ctx, op)
	if err != nil {
		xlog.Error(ctx, "can't create operation", zap.String("operation", types.OperationToString(op)), zap.Error(err))
		return nil, err
	}

	op.Id = operationID
	return op.Proto(), nil
}

func (s *server) ListBackups(ctx context.Context, request *pb.ListBackupsRequest) (*pb.ListBackupsResponse, error) {
	xlog.Debug(ctx, "ListBackups", zap.String("request", request.String()))
	if _, err := s.checkAuth(ctx, auth.PermissionBackupList, request.ContainerId, ""); err != nil {
		return nil, err
	}

	queryFilters := make([]queries.QueryFilter, 0)
	//TODO: forbid empty containerId
	if request.GetContainerId() != "" {
		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field: "container_id",
				Values: []table_types.Value{
					table_types.StringValueFromString(request.ContainerId),
				},
			},
		)
	}
	if request.GetDatabaseNameMask() != "" {
		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field: "database",
				Values: []table_types.Value{
					table_types.StringValueFromString(request.DatabaseNameMask),
				},
				IsLike: true,
			},
		)
	}

	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields(queries.AllBackupFields...),
			queries.WithQueryFilters(queryFilters...),
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
		Backups: pbBackups,
	}, nil
}

func (s *server) ListOperations(ctx context.Context, request *pb.ListOperationsRequest) (
	*pb.ListOperationsResponse, error,
) {
	xlog.Debug(ctx, "ListOperations", zap.String("request", request.String()))
	if _, err := s.checkAuth(ctx, auth.PermissionBackupList, request.ContainerId, ""); err != nil {
		return nil, err
	}

	queryFilters := make([]queries.QueryFilter, 0)
	//TODO: forbid empty containerId
	if request.GetContainerId() != "" {
		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field: "container_id",
				Values: []table_types.Value{
					table_types.StringValueFromString(request.ContainerId),
				},
			},
		)
	}
	if request.GetDatabaseNameMask() != "" {
		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field: "database",
				Values: []table_types.Value{
					table_types.StringValueFromString(request.DatabaseNameMask),
				},
				IsLike: true,
			},
		)
	}

	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithSelectFields(queries.AllOperationFields...),
			queries.WithQueryFilters(queryFilters...),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting operations: %w", err)
	}
	pbOperations := make([]*pb.Operation, 0, len(operations))
	for _, operation := range operations {
		pbOperations = append(pbOperations, operation.Proto())
	}
	return &pb.ListOperationsResponse{
		Operations: pbOperations,
	}, nil
}

func (s *server) CancelOperation(ctx context.Context, request *pb.CancelOperationRequest) (*pb.Operation, error) {
	//TODO implement me
	panic("implement me")
}

func (s *server) GetOperation(ctx context.Context, request *pb.GetOperationRequest) (*pb.Operation, error) {
	xlog.Debug(ctx, "GetOperation", zap.String("request", request.String()))
	requestId, err := types.ParseObjectId(request.GetId())
	if err != nil {
		return nil, fmt.Errorf("failed to parse uuid %s: %w", request.GetId(), err)
	}
	operations, err := s.driver.SelectOperations(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Operations"),
			queries.WithSelectFields(queries.AllOperationFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.UUIDValue(requestId)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select operations", zap.Error(err))
		return nil, err
	}
	if len(operations) == 0 {
		return nil, errors.New("no operation with such Id") // TODO: Permission denied?
	}
	// TODO: Need to check access to operation resource by operationID
	if _, err := s.checkAuth(ctx, auth.PermissionBackupGet, operations[0].GetContainerId(), ""); err != nil {
		return nil, err
	}

	xlog.Debug(ctx, "GetOperation", zap.String("operation", types.OperationToString(operations[0])))
	return operations[0].Proto(), nil
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

	configInstance, err := config.InitConfig(ctx, confPath)

	if err != nil {
		xlog.Error(ctx, "Unable to initialize config", zap.Error(err))
		os.Exit(1)
	}
	confStr, err := configInstance.ToString()
	if err == nil {
		xlog.Debug(
			ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr),
		)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		xlog.Error(ctx, "failed to listen", zap.Error(err))
		return
	}
	s := grpc.NewServer()
	reflection.Register(s)

	dbConnector, err := db.NewYdbConnector(ctx, configInstance.DBConnection)
	if err != nil {
		xlog.Error(ctx, "Error init DBConnector", zap.Error(err))
		os.Exit(1)
	}
	clientConnector := client.NewClientYdbConnector(configInstance.ClientConnection)
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
	defer authProvider.Finish(ctx)

	server := server{
		driver:     dbConnector,
		clientConn: clientConnector,
		s3:         configInstance.S3,
		auth:       authProvider,
	}
	defer server.driver.Close(ctx)

	pb.RegisterBackupServiceServer(s, &server)
	pb.RegisterOperationServiceServer(s, &server)

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
		types.OperationTypeTB,
		handlers.NewTBOperationHandler(dbConnector, clientConnector, configInstance, queries.NewWriteTableQuery),
	)
	if err != nil {
		xlog.Error(ctx, "failed to register TB handler", zap.Error(err))
		return
	}

	err = handlersRegistry.Add(
		types.OperationTypeRB,
		handlers.NewRBOperationHandler(dbConnector, clientConnector, configInstance),
	)

	if err != nil {
		xlog.Error(ctx, "failed to register RB handler", zap.Error(err))
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
