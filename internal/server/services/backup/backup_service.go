package backup

import (
	"context"
	"ydbcp/internal/backup_operations"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	ap "ydbcp/pkg/plugins/auth"

	"ydbcp/internal/auth"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/server"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackupService struct {
	pb.UnimplementedBackupServiceServer
	driver                 db.DBConnector
	clientConn             client.ClientConnector
	s3                     config.S3Config
	auth                   ap.AuthProvider
	allowedEndpointDomains []string
	allowInsecureEndpoint  bool
}

func (s *BackupService) GetBackup(ctx context.Context, request *pb.GetBackupRequest) (*pb.Backup, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, "GetBackup", zap.String("request", request.String()))
	backupID, err := types.ParseObjectID(request.GetId())
	if err != nil {
		xlog.Error(ctx, "failed to parse BackupID", zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, "failed to parse BackupID")
	}
	ctx = xlog.With(ctx, zap.String("BackupID", backupID))
	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields(queries.AllBackupFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(backupID)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return nil, status.Error(codes.Internal, "can't select backups")
	}
	if len(backups) == 0 {
		xlog.Error(ctx, "backup not found")
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission denied?
	}
	backup := backups[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", backup.ContainerID))
	// TODO: Need to check access to backup resource by backupID
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, backup.ContainerID, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	xlog.Debug(ctx, "GetBackup", zap.String("backup", backup.String()))
	return backups[0].Proto(), nil
}

func (s *BackupService) MakeBackup(ctx context.Context, req *pb.MakeBackupRequest) (*pb.Operation, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)

	xlog.Info(ctx, "MakeBackup", zap.String("request", req.String()))
	ctx = xlog.With(ctx, zap.String("ContainerID", req.ContainerId))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, req.ContainerId, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	backup, op, err := backup_operations.MakeBackup(
		ctx, s.clientConn, s.s3, s.allowedEndpointDomains, s.allowInsecureEndpoint, req, nil, subject,
	)

	if err != nil {
		return nil, err
	}
	err = s.driver.ExecuteUpsert(
		ctx, queries.NewWriteTableQuery().WithCreateBackup(*backup).WithCreateOperation(op),
	)
	if err != nil {
		return nil, err
	}
	return op.Proto(), nil
}

func (s *BackupService) DeleteBackup(ctx context.Context, req *pb.DeleteBackupRequest) (*pb.Operation, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Info(ctx, "DeleteBackup", zap.String("request", req.String()))

	backupID, err := types.ParseObjectID(req.BackupId)
	if err != nil {
		xlog.Error(ctx, "failed to parse BackupID", zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, "failed to parse BackupID")
	}
	ctx = xlog.With(ctx, zap.String("BackupID", backupID))

	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields(queries.AllBackupFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(backupID)},
				},
			),
		),
	)

	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return nil, status.Error(codes.Internal, "can't select backups")
	}

	if len(backups) == 0 {
		xlog.Error(ctx, "backup not found")
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission Denied?
	}

	backup := backups[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", backup.ContainerID))

	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, backup.ContainerID, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if !backup.CanBeDeleted() {
		xlog.Error(ctx, "backup can't be deleted", zap.String("BackupStatus", backup.Status))
		return nil, status.Errorf(codes.FailedPrecondition, "backup can't be deleted, status %s", backup.Status)
	}

	now := timestamppb.Now()
	op := &types.DeleteBackupOperation{
		ContainerID: backup.ContainerID,
		BackupID:    req.GetBackupId(),
		State:       types.OperationStatePending,
		YdbConnectionParams: types.YdbConnectionParams{
			DatabaseName: backup.DatabaseName,
			Endpoint:     backup.DatabaseEndpoint,
		},
		Audit: &pb.AuditInfo{
			CreatedAt: now,
			Creator:   subject,
		},
		PathPrefix: backup.S3PathPrefix,
		UpdatedAt:  now,
	}

	operationID, err := s.driver.CreateOperation(ctx, op)
	if err != nil {
		xlog.Error(ctx, "can't create operation", zap.Error(err))
		return nil, status.Error(codes.Internal, "can't create operation")
	}
	ctx = xlog.With(ctx, zap.String("OperationID", operationID))

	op.ID = operationID
	xlog.Debug(ctx, "DeleteBackup was started successfully", zap.String("operation", types.OperationToString(op)))
	return op.Proto(), nil
}

func (s *BackupService) MakeRestore(ctx context.Context, req *pb.MakeRestoreRequest) (*pb.Operation, error) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Info(ctx, "MakeRestore", zap.String("request", req.String()))

	backupID, err := types.ParseObjectID(req.BackupId)
	if err != nil {
		xlog.Error(ctx, "failed to parse BackupID", zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, "failed to parse BackupID")
	}
	ctx = xlog.With(ctx, zap.String("BackupID", backupID))

	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields(queries.AllBackupFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(backupID)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return nil, status.Error(codes.Internal, "can't select backups")
	}
	if len(backups) == 0 {
		xlog.Error(ctx, "backup not found")
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission denied?
	}
	backup := backups[0]

	ctx = xlog.With(ctx, zap.String("ContainerID", backup.ContainerID))
	subject, err := auth.CheckAuth(
		ctx, s.auth, auth.PermissionBackupRestore, backup.ContainerID, "",
	) // TODO: check access to backup as resource
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if !backup_operations.IsAllowedEndpoint(req.DatabaseEndpoint, s.allowedEndpointDomains, s.allowInsecureEndpoint) {
		xlog.Error(
			ctx, "endpoint of database is invalid or not allowed", zap.String("DatabaseEndpoint", req.DatabaseEndpoint),
		)
		return nil, status.Errorf(
			codes.InvalidArgument, "endpoint of database is invalid or not allowed, endpoint %s", req.DatabaseEndpoint,
		)
	}

	if backup.Status != types.BackupStateAvailable {
		xlog.Error(ctx, "backup is not available", zap.String("BackupStatus", backup.Status))
		return nil, status.Errorf(codes.FailedPrecondition, "backup is not available, status %s", backup.Status)
	}

	clientConnectionParams := types.YdbConnectionParams{
		Endpoint:     req.DatabaseEndpoint,
		DatabaseName: req.DatabaseName,
	}
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	ctx = xlog.With(ctx, zap.String("ClientDSN", dsn))
	client, err := s.clientConn.Open(ctx, dsn)
	if err != nil {
		xlog.Error(ctx, "can't open client connection", zap.Error(err))
		return nil, status.Errorf(codes.Unknown, "can't open client connection, dsn %s", dsn)
	}
	defer func() {
		if err := s.clientConn.Close(ctx, client); err != nil {
			xlog.Error(ctx, "can't close client connection", zap.Error(err))
		}
	}()

	accessKey, err := s.s3.AccessKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3AccessKey", zap.Error(err))
		return nil, status.Error(codes.Internal, "can't get S3AccessKey")
	}
	secretKey, err := s.s3.SecretKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3SecretKey", zap.Error(err))
		return nil, status.Error(codes.Internal, "can't get S3SecretKey")
	}

	var sourcePaths []string
	if len(req.SourcePaths) == 0 {
		sourcePaths = []string{backup.S3PathPrefix}
	} else {
		sourcePaths = make([]string, 0, len(req.SourcePaths))
		for _, p := range req.SourcePaths {
			fullPath, ok := backup_operations.SafePathJoin(backup.S3PathPrefix, p)
			if !ok {
				xlog.Error(ctx, "incorrect source path", zap.String("path", p))
				return nil, status.Errorf(codes.InvalidArgument, "incorrect source path %s", p)
			}
			sourcePaths = append(sourcePaths, fullPath)
		}
	}

	s3Settings := types.ImportSettings{
		Endpoint:          s.s3.Endpoint,
		Region:            s.s3.Region,
		Bucket:            s.s3.Bucket,
		AccessKey:         accessKey,
		SecretKey:         secretKey,
		Description:       "ydbcp restore", // TODO: write description
		NumberOfRetries:   10,              // TODO: get value from configuration
		BackupID:          backupID,
		SourcePaths:       sourcePaths,
		S3ForcePathStyle:  s.s3.S3ForcePathStyle,
		DestinationPrefix: req.GetDestinationPrefix(),
	}

	clientOperationID, err := s.clientConn.ImportFromS3(ctx, client, s3Settings)
	if err != nil {
		xlog.Error(ctx, "can't start import operation", zap.Error(err))
		return nil, status.Errorf(codes.Unknown, "can't start import operation, dsn %s", dsn)
	}
	ctx = xlog.With(ctx, zap.String("ClientOperationID", clientOperationID))
	xlog.Debug(ctx, "import operation started")

	now := timestamppb.Now()
	op := &types.RestoreBackupOperation{
		ContainerID: backup.ContainerID,
		BackupId:    backupID,
		State:       types.OperationStateRunning,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.GetDatabaseEndpoint(),
			DatabaseName: req.GetDatabaseName(),
		},
		YdbOperationId: clientOperationID,
		Audit: &pb.AuditInfo{
			CreatedAt: now,
			Creator:   subject,
		},
		SourcePaths:       req.GetSourcePaths(),
		DestinationPrefix: req.GetDestinationPrefix(),
		UpdatedAt:         now,
	}

	operationID, err := s.driver.CreateOperation(ctx, op)
	if err != nil {
		xlog.Error(ctx, "can't create operation", zap.String("operation", types.OperationToString(op)), zap.Error(err))
		return nil, status.Error(codes.Internal, "can't create operation")
	}
	ctx = xlog.With(ctx, zap.String("OperationID", operationID))
	xlog.Info(ctx, "RestoreBackup operation created")

	op.ID = operationID
	return op.Proto(), nil
}

func (s *BackupService) ListBackups(ctx context.Context, request *pb.ListBackupsRequest) (
	*pb.ListBackupsResponse, error,
) {
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, "ListBackups", zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("ContainerID", request.ContainerId))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, "")
	if err != nil {
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

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
		xlog.Error(ctx, "error getting backups", zap.Error(err))
		return nil, status.Error(codes.Internal, "error getting backups")
	}
	pbBackups := make([]*pb.Backup, 0, len(backups))
	for _, backup := range backups {
		pbBackups = append(pbBackups, backup.Proto())
	}
	xlog.Debug(ctx, "success")
	return &pb.ListBackupsResponse{Backups: pbBackups}, nil
}

func (s *BackupService) Register(server server.Server) {
	pb.RegisterBackupServiceServer(server.GRPCServer(), s)
}

func NewBackupService(
	driver db.DBConnector,
	clientConn client.ClientConnector,
	s3 config.S3Config,
	auth ap.AuthProvider,
	allowedEndpointDomains []string,
	allowInsecureEndpoint bool,
) *BackupService {
	return &BackupService{
		driver:                 driver,
		clientConn:             clientConn,
		s3:                     s3,
		auth:                   auth,
		allowedEndpointDomains: allowedEndpointDomains,
		allowInsecureEndpoint:  allowInsecureEndpoint,
	}
}
