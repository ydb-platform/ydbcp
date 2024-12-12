package backup

import (
	"context"
	"github.com/jonboulle/clockwork"
	"strconv"
	"time"
	"ydbcp/internal/auth"
	"ydbcp/internal/backup_operations"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"
	"ydbcp/internal/connectors/db/yql/queries"
	"ydbcp/internal/metrics"
	"ydbcp/internal/server"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	ap "ydbcp/pkg/plugins/auth"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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
	clock                  clockwork.Clock
}

func (s *BackupService) IncApiCallsCounter(methodName string, code codes.Code) {
	metrics.GlobalMetricsRegistry.IncApiCallsCounter("BackupService", methodName, code.String())
}

func (s *BackupService) GetBackup(ctx context.Context, request *pb.GetBackupRequest) (*pb.Backup, error) {
	const methodName string = "GetBackup"
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, methodName, zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("BackupID", request.Id))
	backupID, err := types.ParseObjectID(request.GetId())
	if err != nil {
		xlog.Error(ctx, "failed to parse BackupID", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.InvalidArgument)
		return nil, status.Error(codes.InvalidArgument, "failed to parse BackupID")
	}
	ctx = xlog.With(ctx, zap.String("BackupID", backupID))
	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't select backups")
	}
	if len(backups) == 0 {
		xlog.Error(ctx, "backup not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission denied?
	}
	backup := backups[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", backup.ContainerID))
	// TODO: Need to check access to backup resource by backupID
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupGet, backup.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	xlog.Debug(ctx, methodName, zap.String("backup", backup.String()))
	s.IncApiCallsCounter(methodName, codes.OK)
	return backups[0].Proto(), nil
}

func (s *BackupService) MakeBackup(ctx context.Context, req *pb.MakeBackupRequest) (*pb.Operation, error) {
	const methodName string = "MakeBackup"
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, methodName, zap.String("request", req.String()))

	ctx = xlog.With(ctx, zap.String("ContainerID", req.ContainerId))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, req.ContainerId, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))
	now := timestamppb.Now()

	tbwr := &types.TakeBackupWithRetryOperation{
		TakeBackupOperation: types.TakeBackupOperation{
			ID:          types.GenerateObjectID(),
			ContainerID: req.ContainerId,
			State:       types.OperationStateRunning,
			YdbConnectionParams: types.YdbConnectionParams{
				Endpoint:     req.DatabaseEndpoint,
				DatabaseName: req.DatabaseName,
			},
			SourcePaths:          req.SourcePaths,
			SourcePathsToExclude: req.SourcePathsToExclude,
			Audit: &pb.AuditInfo{
				Creator:   subject,
				CreatedAt: now,
			},
			UpdatedAt: now,
		},
		Retries: 0,
		RetryConfig: &pb.RetryConfig{
			Retries: &pb.RetryConfig_Count{Count: 3},
		},
	}
	if d := req.Ttl.AsDuration(); req.Ttl != nil {
		tbwr.Ttl = &d
	}

	_, err = backup_operations.OpenConnAndValidateSourcePaths(ctx, backup_operations.FromTBWROperation(tbwr), s.clientConn)
	if err != nil {
		grpcError := backup_operations.ErrToStatus(err)
		s.IncApiCallsCounter(methodName, status.Code(grpcError))
		return nil, grpcError
	}

	err = s.driver.ExecuteUpsert(
		ctx, queries.NewWriteTableQuery().WithCreateOperation(tbwr),
	)
	if err != nil {
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = xlog.With(ctx, zap.String("BackupID", tbwr.BackupID))
	ctx = xlog.With(ctx, zap.String("OperationID", tbwr.GetID()))
	xlog.Debug(ctx, methodName, zap.String("operation", types.OperationToString(tbwr)))
	s.IncApiCallsCounter(methodName, codes.OK)
	return tbwr.Proto(), nil
}

func (s *BackupService) DeleteBackup(ctx context.Context, req *pb.DeleteBackupRequest) (*pb.Operation, error) {
	const methodName string = "DeleteBackup"
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, methodName, zap.String("request", req.String()))
	ctx = xlog.With(ctx, zap.String("BackupID", req.BackupId))

	backupID, err := types.ParseObjectID(req.BackupId)
	if err != nil {
		xlog.Error(ctx, "failed to parse BackupID", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.InvalidArgument)
		return nil, status.Error(codes.InvalidArgument, "failed to parse BackupID")
	}
	ctx = xlog.With(ctx, zap.String("BackupID", backupID))

	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't select backups")
	}

	if len(backups) == 0 {
		xlog.Error(ctx, "backup not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission Denied?
	}

	backup := backups[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", backup.ContainerID))

	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, backup.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if !backup.CanBeDeleted() {
		xlog.Error(ctx, "backup can't be deleted", zap.String("BackupStatus", backup.Status))
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Errorf(codes.FailedPrecondition, "backup can't be deleted, status %s", backup.Status)
	}

	now := timestamppb.Now()
	op := &types.DeleteBackupOperation{
		ID:          types.GenerateObjectID(),
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

	backup.Status = types.BackupStateDeleting
	err = s.driver.ExecuteUpsert(
		ctx, queries.NewWriteTableQuery().WithCreateOperation(op).WithUpdateBackup(*backup),
	)
	if err != nil {
		xlog.Error(ctx, "can't create operation", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't create operation")
	}

	ctx = xlog.With(ctx, zap.String("OperationID", op.GetID()))
	xlog.Debug(ctx, methodName, zap.String("operation", types.OperationToString(op)))
	s.IncApiCallsCounter(methodName, codes.OK)
	return op.Proto(), nil
}

func (s *BackupService) MakeRestore(ctx context.Context, req *pb.MakeRestoreRequest) (*pb.Operation, error) {
	const methodName string = "MakeRestore"
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, methodName, zap.String("request", req.String()))
	ctx = xlog.With(ctx, zap.String("BackupID", req.BackupId))

	backupID, err := types.ParseObjectID(req.BackupId)
	if err != nil {
		xlog.Error(ctx, "failed to parse BackupID", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.InvalidArgument)
		return nil, status.Error(codes.InvalidArgument, "failed to parse BackupID")
	}
	ctx = xlog.With(ctx, zap.String("BackupID", backupID))

	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't select backups")
	}
	if len(backups) == 0 {
		xlog.Error(ctx, "backup not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission denied?
	}
	backup := backups[0]

	ctx = xlog.With(ctx, zap.String("ContainerID", backup.ContainerID))
	subject, err := auth.CheckAuth(
		ctx, s.auth, auth.PermissionBackupRestore, backup.ContainerID, "",
	) // TODO: check access to backup as resource
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if !backup_operations.IsAllowedEndpoint(req.DatabaseEndpoint, s.allowedEndpointDomains, s.allowInsecureEndpoint) {
		xlog.Error(
			ctx,
			"endpoint of database is invalid or not allowed",
			zap.String("DatabaseEndpoint", req.DatabaseEndpoint),
		)
		s.IncApiCallsCounter(methodName, codes.InvalidArgument)
		return nil, status.Errorf(
			codes.InvalidArgument, "endpoint of database is invalid or not allowed, endpoint %s", req.DatabaseEndpoint,
		)
	}

	if backup.Status != types.BackupStateAvailable {
		xlog.Error(ctx, "backup is not available", zap.String("BackupStatus", backup.Status))
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
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
		s.IncApiCallsCounter(methodName, codes.Unknown)
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't get S3AccessKey")
	}
	secretKey, err := s.s3.SecretKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3SecretKey", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
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
				s.IncApiCallsCounter(methodName, codes.InvalidArgument)
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
		s.IncApiCallsCounter(methodName, codes.Unknown)
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't create operation")
	}
	ctx = xlog.With(ctx, zap.String("OperationID", operationID))
	op.ID = operationID

	xlog.Debug(ctx, methodName, zap.String("operation", types.OperationToString(op)))
	s.IncApiCallsCounter(methodName, codes.OK)
	return op.Proto(), nil
}

func (s *BackupService) ListBackups(ctx context.Context, request *pb.ListBackupsRequest) (
	*pb.ListBackupsResponse, error,
) {
	const methodName string = "ListBackups"
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, methodName, zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("ContainerID", request.ContainerId))
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupList, request.ContainerId, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
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
	if len(request.DisplayStatus) > 0 {
		var displayStatuses []table_types.Value
		for _, backupStatus := range request.DisplayStatus {
			displayStatuses = append(
				displayStatuses,
				table_types.StringValueFromString(backupStatus.String()),
			)
		}
		queryFilters = append(
			queryFilters, queries.QueryFilter{
				Field:  "status",
				Values: displayStatuses,
			},
		)
	}
	pageSpec, err := queries.NewPageSpec(request.GetPageSize(), request.GetPageToken())
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	orderSpec, err := queries.NewOrderSpec(request.GetOrder())
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}

	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
			queries.WithQueryFilters(queryFilters...),
			queries.WithOrderBy(*orderSpec),
			queries.WithPageSpec(*pageSpec),
		),
	)
	if err != nil {
		xlog.Error(ctx, "error getting backups", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "error getting backups")
	}
	pbBackups := make([]*pb.Backup, 0, len(backups))
	for _, backup := range backups {
		pbBackups = append(pbBackups, backup.Proto())
	}
	res := &pb.ListBackupsResponse{
		Backups: pbBackups,
	}
	if uint64(len(pbBackups)) == pageSpec.Limit {
		res.NextPageToken = strconv.FormatUint(pageSpec.Offset+pageSpec.Limit, 10)
	}
	xlog.Debug(ctx, methodName, zap.String("response", res.String()))
	s.IncApiCallsCounter(methodName, codes.OK)
	return res, nil
}

func (s *BackupService) UpdateBackupTtl(ctx context.Context, request *pb.UpdateBackupTtlRequest) (
	*pb.Backup, error,
) {
	const methodName string = "UpdateBackupTtl"
	ctx = grpcinfo.WithGRPCInfo(ctx)
	xlog.Debug(ctx, methodName, zap.String("request", request.String()))
	ctx = xlog.With(ctx, zap.String("BackupID", request.BackupId))
	backupID, err := types.ParseObjectID(request.GetBackupId())
	if err != nil {
		xlog.Error(ctx, "failed to parse BackupID", zap.Error(err))
		s.IncApiCallsCounter(methodName, codes.InvalidArgument)
		return nil, status.Error(codes.InvalidArgument, "failed to parse BackupID")
	}
	ctx = xlog.With(ctx, zap.String("BackupID", backupID))
	backups, err := s.driver.SelectBackups(
		ctx, queries.NewReadTableQuery(
			queries.WithTableName("Backups"),
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
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, "can't select backups")
	}
	if len(backups) == 0 {
		xlog.Error(ctx, "backup not found")
		s.IncApiCallsCounter(methodName, codes.NotFound)
		return nil, status.Error(codes.NotFound, "backup not found") // TODO: Permission denied?
	}
	backup := backups[0]
	ctx = xlog.With(ctx, zap.String("ContainerID", backup.ContainerID))
	// TODO: Need to check access to backup resource by backupID
	subject, err := auth.CheckAuth(ctx, s.auth, auth.PermissionBackupCreate, backup.ContainerID, "")
	if err != nil {
		s.IncApiCallsCounter(methodName, status.Code(err))
		return nil, err
	}
	ctx = xlog.With(ctx, zap.String("SubjectID", subject))

	if backup.Status != types.BackupStateAvailable {
		xlog.Error(ctx, "backup is not available", zap.String("BackupStatus", backup.Status))
		s.IncApiCallsCounter(methodName, codes.FailedPrecondition)
		return nil, status.Errorf(codes.FailedPrecondition, "backup is not available, status %s", backup.Status)
	}

	var expireAt *time.Time = nil
	if request.Ttl != nil {
		expireAt = new(time.Time)
		*expireAt = s.clock.Now().Add(request.Ttl.AsDuration())
	}

	backup.ExpireAt = expireAt
	err = s.driver.ExecuteUpsert(
		ctx, queries.NewWriteTableQuery().WithUpdateBackup(*backup),
	)

	if err != nil {
		s.IncApiCallsCounter(methodName, codes.Internal)
		return nil, status.Error(codes.Internal, err.Error())
	}

	xlog.Debug(ctx, methodName, zap.Stringer("backup", backup))
	s.IncApiCallsCounter(methodName, codes.OK)
	return backup.Proto(), nil
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
		clock:                  clockwork.NewRealClock(),
	}
}
