package backup

import (
	"context"
	"path"
	"regexp"
	"strings"
	"time"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ydbcp/internal/auth"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/server"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
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

var (
	validEndpoint = regexp.MustCompile(`^(grpcs://|grpc://)?([A-Za-z0-9\-\.]+)(:[0-9]+)?$`)
)

func (s *BackupService) isAllowedEndpoint(e string) bool {
	groups := validEndpoint.FindStringSubmatch(e)
	if len(groups) < 3 {
		return false
	}
	tls := groups[1] == "grpcs://"
	if !tls && !s.allowInsecureEndpoint {
		return false
	}
	fqdn := groups[2]

	for _, domain := range s.allowedEndpointDomains {
		if strings.HasPrefix(domain, ".") {
			if strings.HasSuffix(fqdn, domain) {
				return true
			}
		} else if fqdn == domain {
			return true
		}
	}
	return false
}

func safePathJoin(base string, relPath ...string) (fullPath string, ok bool) {
	paths := append([]string{base}, relPath...)
	fullPath = path.Join(paths...)
	if strings.HasPrefix(fullPath, base+"/") {
		return fullPath, true
	}
	return "", false // Possible Path Traversal
}

func (s *BackupService) GetBackup(ctx context.Context, request *pb.GetBackupRequest) (*pb.Backup, error) {
	xlog.Debug(ctx, "GetBackup", zap.String("request", request.String()))
	requestId, err := types.ParseObjectID(request.GetId())
	if err != nil {
		xlog.Error(ctx, "failed to parse ObjectID", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse ObjectID %s: %v", request.GetId(), err)
	}
	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields(queries.AllBackupFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(requestId)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "can't select backups: %v", err)
	}
	if len(backups) == 0 {
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission denied?
	}
	// TODO: Need to check access to backup resource by backupID
	if _, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, backups[0].ContainerID, ""); err != nil {
		return nil, err
	}

	xlog.Debug(ctx, "GetBackup", zap.String("backup", backups[0].String()))
	return backups[0].Proto(), nil
}

func (s *BackupService) MakeBackup(ctx context.Context, req *pb.MakeBackupRequest) (*pb.Operation, error) {
	xlog.Info(ctx, "MakeBackup", zap.String("request", req.String()))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, req.ContainerId, "")
	if err != nil {
		return nil, err
	}
	xlog.Debug(ctx, "MakeBackup", zap.String("subject", subject))

	if !s.isAllowedEndpoint(req.DatabaseEndpoint) {
		return nil, status.Errorf(codes.InvalidArgument, "endpoint of database is invalid or not allowed, endpoint %s", req.DatabaseEndpoint)
	}

	clientConnectionParams := types.YdbConnectionParams{
		Endpoint:     req.DatabaseEndpoint,
		DatabaseName: req.DatabaseName,
	}
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	client, err := s.clientConn.Open(ctx, dsn)
	if err != nil {
		xlog.Error(ctx, "can't open client connection", zap.Error(err), zap.String("dsn", dsn))
		return nil, status.Errorf(codes.Unknown, "can't open client connection, dsn %s: %v", dsn, err)
	}
	defer func() {
		if err := s.clientConn.Close(ctx, client); err != nil {
			xlog.Error(ctx, "can't close client connection", zap.Error(err))
		}
	}()

	accessKey, err := s.s3.AccessKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3AccessKey", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "can't get S3AccessKey: %v", err)
	}
	secretKey, err := s.s3.SecretKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3SecretKey", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "can't get S3SecretKey: %v", err)
	}

	dbNamePath := strings.Replace(req.DatabaseName, "/", "_", -1) // TODO: checking user input
	dbNamePath = strings.Trim(dbNamePath, "_")

	destinationPrefix := path.Join(
		s.s3.PathPrefix,
		dbNamePath,
		time.Now().Format(types.BackupTimestampFormat),
	)

	sourcePaths := make([]string, 0, len(req.SourcePaths))
	for _, p := range req.SourcePaths {
		fullPath, ok := safePathJoin(req.DatabaseName, p)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "incorrect source path %s", p)
		}
		sourcePaths = append(sourcePaths, fullPath)
	}

	s3Settings := types.ExportSettings{
		Endpoint:            s.s3.Endpoint,
		Region:              s.s3.Region,
		Bucket:              s.s3.Bucket,
		AccessKey:           accessKey,
		SecretKey:           secretKey,
		Description:         "ydbcp backup", // TODO: the description shoud be better
		NumberOfRetries:     10,             // TODO: get it from configuration
		SourcePaths:         sourcePaths,
		SourcePathToExclude: req.GetSourcePathsToExclude(),
		DestinationPrefix:   destinationPrefix,
		S3ForcePathStyle:    s.s3.S3ForcePathStyle,
	}

	clientOperationID, err := s.clientConn.ExportToS3(ctx, client, s3Settings)
	if err != nil {
		xlog.Error(ctx, "can't start export operation", zap.Error(err), zap.String("dns", dsn))
		return nil, status.Errorf(codes.Unknown, "can't start export operation, dsn %s: %v", dsn, err)
	}
	xlog.Debug(
		ctx, "export operation started", zap.String("clientOperationID", clientOperationID), zap.String("dsn", dsn),
	)

	now := timestamppb.Now()
	backup := types.Backup{
		ContainerID:      req.GetContainerId(),
		DatabaseName:     req.GetDatabaseName(),
		DatabaseEndpoint: req.GetDatabaseEndpoint(),
		S3Endpoint:       s.s3.Endpoint,
		S3Region:         s.s3.Region,
		S3Bucket:         s.s3.Bucket,
		S3PathPrefix:     destinationPrefix,
		Status:           types.BackupStateRunning,
		AuditInfo: &pb.AuditInfo{
			CreatedAt: now,
			Creator:   subject,
		},
	}
	backupID, err := s.driver.CreateBackup(ctx, backup)
	if err != nil {
		xlog.Error(
			ctx, "can't create backup",
			zap.String("backup", backup.String()),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "can't create backup: %v", err)
	}

	op := &types.TakeBackupOperation{
		BackupId:    backupID,
		ContainerID: req.ContainerId,
		State:       types.OperationStateRunning,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.GetDatabaseEndpoint(),
			DatabaseName: req.GetDatabaseName(),
		},
		SourcePaths:         req.GetSourcePaths(),
		SourcePathToExclude: req.GetSourcePathsToExclude(),
		Audit: &pb.AuditInfo{
			CreatedAt: now,
			Creator:   subject,
		},
		YdbOperationId: clientOperationID,
	}

	operationID, err := s.driver.CreateOperation(ctx, op)
	if err != nil {
		xlog.Error(ctx, "can't create operation", zap.String("operation", types.OperationToString(op)), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "can't create operation: %v", err)
	}

	op.ID = operationID
	return op.Proto(), nil
}

func (s *BackupService) DeleteBackup(ctx context.Context, req *pb.DeleteBackupRequest) (*pb.Operation, error) {
	xlog.Info(ctx, "DeleteBackup", zap.String("request", req.String()))

	backupID, err := types.ParseObjectID(req.BackupId)
	if err != nil {
		xlog.Error(ctx, "failed to parse BackupId", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse BackupId %s: %v", req.BackupId, err)
	}

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
		return nil, status.Errorf(codes.Internal, "can't select backups: %v", err)
	}

	if len(backups) == 0 {
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission Denied?
	}

	backup := backups[0]

	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, backup.ContainerID, "")
	if err != nil {
		return nil, err
	}

	if !backup.CanBeDeleted() {
		return nil, status.Errorf(codes.FailedPrecondition, "backup can't be deleted, status %s", backup.Status)
	}

	op := &types.DeleteBackupOperation{
		ContainerID: backup.ContainerID,
		BackupID:    req.GetBackupId(),
		State:       types.OperationStatePending,
		YdbConnectionParams: types.YdbConnectionParams{
			DatabaseName: backup.DatabaseName,
			Endpoint:     backup.DatabaseEndpoint,
		},
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
			Creator:   subject,
		},
		PathPrefix: backup.S3PathPrefix,
	}

	operationID, err := s.driver.CreateOperation(ctx, op)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "can't create operation: %v", err)
	}

	op.ID = operationID
	xlog.Debug(ctx, "DeleteBackup was started successfully", zap.String("operation", types.OperationToString(op)))
	return op.Proto(), nil
}

func (s *BackupService) MakeRestore(ctx context.Context, req *pb.MakeRestoreRequest) (*pb.Operation, error) {
	xlog.Info(ctx, "MakeRestore", zap.String("request", req.String()))

	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupRestore, req.ContainerId, "") // TODO: check access to backup as resource
	if err != nil {
		return nil, err
	}
	xlog.Debug(ctx, "MakeRestore", zap.String("subject", subject))

	if !s.isAllowedEndpoint(req.DatabaseEndpoint) {
		return nil, status.Errorf(codes.InvalidArgument, "endpoint of database is invalid or not allowed, endpoint %s", req.DatabaseEndpoint)
	}

	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithSelectFields(queries.AllBackupFields...),
			queries.WithQueryFilters(
				queries.QueryFilter{
					Field:  "id",
					Values: []table_types.Value{table_types.StringValueFromString(req.BackupId)},
				},
			),
		),
	)
	if err != nil {
		xlog.Error(ctx, "can't select backups", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "can't select backups: %v", err)
	}
	if len(backups) == 0 {
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission denied?
	}
	backup := backups[0]

	if backup.Status != types.BackupStateAvailable {
		return nil, status.Errorf(codes.FailedPrecondition, "backup is not available, status %s", backup.Status)
	}

	clientConnectionParams := types.YdbConnectionParams{
		Endpoint:     req.DatabaseEndpoint,
		DatabaseName: req.DatabaseName,
	}
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	client, err := s.clientConn.Open(ctx, dsn)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "can't open client connection, dsn %s: %v", dsn, err)
	}
	defer func() {
		if err := s.clientConn.Close(ctx, client); err != nil {
			xlog.Error(ctx, "can't close client connection", zap.Error(err))
		}
	}()

	accessKey, err := s.s3.AccessKey()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "can't get S3AccessKey: %v", err)
	}
	secretKey, err := s.s3.SecretKey()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "can't get S3SecretKey: %v", err)
	}

	var sourcePaths []string
	if len(req.SourcePaths) == 0 {
		sourcePaths = []string{backup.S3PathPrefix}
	} else {
		sourcePaths = make([]string, 0, len(req.SourcePaths))
		for _, p := range req.SourcePaths {
			fullPath, ok := safePathJoin(backup.S3PathPrefix, p)
			if !ok {
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
		BackupID:          req.GetBackupId(),
		SourcePaths:       sourcePaths,
		S3ForcePathStyle:  s.s3.S3ForcePathStyle,
		DestinationPrefix: req.GetDestinationPrefix(),
	}

	clientOperationID, err := s.clientConn.ImportFromS3(ctx, client, s3Settings)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "can't start import operation, dsn %s: %v", dsn, err)
	}

	xlog.Debug(
		ctx, "import operation started", zap.String("clientOperationID", clientOperationID), zap.String("dsn", dsn),
	)

	op := &types.RestoreBackupOperation{
		ContainerID: req.GetContainerId(),
		BackupId:    req.GetBackupId(),
		State:       types.OperationStateRunning,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.GetDatabaseEndpoint(),
			DatabaseName: req.GetDatabaseName(),
		},
		YdbOperationId: clientOperationID,
		Audit: &pb.AuditInfo{
			CreatedAt: timestamppb.Now(),
			Creator:   subject,
		},
		SourcePaths:       req.GetSourcePaths(),
		DestinationPrefix: req.GetDestinationPrefix(),
	}

	operationID, err := s.driver.CreateOperation(ctx, op)
	if err != nil {
		xlog.Error(ctx, "can't create operation", zap.String("operation", types.OperationToString(op)), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "can't create operation: %v", err)
	}

	op.ID = operationID
	return op.Proto(), nil
}

func (s *BackupService) ListBackups(ctx context.Context, request *pb.ListBackupsRequest) (
	*pb.ListBackupsResponse, error,
) {
	xlog.Debug(ctx, "ListBackups", zap.String("request", request.String()))
	if _, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, ""); err != nil {
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
		return nil, status.Errorf(codes.Internal, "error getting backups: %v", err)
	}
	pbBackups := make([]*pb.Backup, 0, len(backups))
	for _, backup := range backups {
		pbBackups = append(pbBackups, backup.Proto())
	}
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
