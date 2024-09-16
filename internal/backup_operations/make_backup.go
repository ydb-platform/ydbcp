package backup_operations

import (
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"path"
	"regexp"
	"strings"
	"time"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

var (
	validEndpoint = regexp.MustCompile(`^(grpcs://|grpc://)?([A-Za-z0-9\-\.]+)(:[0-9]+)?$`)
)

func SafePathJoin(base string, relPath ...string) (fullPath string, ok bool) {
	paths := append([]string{base}, relPath...)
	fullPath = path.Join(paths...)
	if strings.HasPrefix(fullPath, base+"/") {
		return fullPath, true
	}
	return "", false // Possible Path Traversal
}

func IsAllowedEndpoint(e string, allowedEndpointDomains []string, allowInsecureEndpoint bool) bool {
	groups := validEndpoint.FindStringSubmatch(e)
	if len(groups) < 3 {
		return false
	}
	tls := groups[1] == "grpcs://"
	if !tls && !allowInsecureEndpoint {
		return false
	}
	fqdn := groups[2]

	for _, domain := range allowedEndpointDomains {
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

func MakeBackup(
	ctx context.Context,
	clientConn client.ClientConnector,
	s3 config.S3Config,
	allowedEndpointDomains []string,
	allowInsecureEndpoint bool,
	req *pb.MakeBackupRequest, scheduleId *string,
	subject string,
) (*types.Backup, *types.TakeBackupOperation, error) {
	if !IsAllowedEndpoint(req.DatabaseEndpoint, allowedEndpointDomains, allowInsecureEndpoint) {
		xlog.Error(
			ctx,
			"endpoint of database is invalid or not allowed",
			zap.String("DatabaseEndpoint", req.DatabaseEndpoint),
		)
		return nil, nil, status.Errorf(
			codes.InvalidArgument,
			"endpoint of database is invalid or not allowed, endpoint %s", req.DatabaseEndpoint,
		)
	}

	clientConnectionParams := types.YdbConnectionParams{
		Endpoint:     req.DatabaseEndpoint,
		DatabaseName: req.DatabaseName,
	}
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	ctx = xlog.With(ctx, zap.String("ClientDSN", dsn))
	client, err := clientConn.Open(ctx, dsn)
	if err != nil {
		xlog.Error(ctx, "can't open client connection", zap.Error(err))
		return nil, nil, status.Errorf(codes.Unknown, "can't open client connection, dsn %s", dsn)
	}
	defer func() {
		if err := clientConn.Close(ctx, client); err != nil {
			xlog.Error(ctx, "can't close client connection", zap.Error(err))
		}
	}()

	accessKey, err := s3.AccessKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3AccessKey", zap.Error(err))
		return nil, nil, status.Error(codes.Internal, "can't get S3AccessKey")
	}
	secretKey, err := s3.SecretKey()
	if err != nil {
		xlog.Error(ctx, "can't get S3SecretKey", zap.Error(err))
		return nil, nil, status.Error(codes.Internal, "can't get S3SecretKey")
	}

	dbNamePath := strings.Replace(req.DatabaseName, "/", "_", -1) // TODO: checking user input
	dbNamePath = strings.Trim(dbNamePath, "_")

	destinationPrefix := path.Join(
		s3.PathPrefix,
		dbNamePath,
		time.Now().Format(types.BackupTimestampFormat),
	)
	ctx = xlog.With(ctx, zap.String("S3DestinationPrefix", destinationPrefix))

	sourcePaths := make([]string, 0, len(req.SourcePaths))
	for _, p := range req.SourcePaths {
		fullPath, ok := SafePathJoin(req.DatabaseName, p)
		if !ok {
			xlog.Error(ctx, "incorrect source path", zap.String("path", p))
			return nil, nil, status.Errorf(codes.InvalidArgument, "incorrect source path %s", p)
		}
		sourcePaths = append(sourcePaths, fullPath)
	}

	s3Settings := types.ExportSettings{
		Endpoint:            s3.Endpoint,
		Region:              s3.Region,
		Bucket:              s3.Bucket,
		AccessKey:           accessKey,
		SecretKey:           secretKey,
		Description:         "ydbcp backup", // TODO: the description shoud be better
		NumberOfRetries:     10,             // TODO: get it from configuration
		SourcePaths:         sourcePaths,
		SourcePathToExclude: req.GetSourcePathsToExclude(),
		DestinationPrefix:   destinationPrefix,
		S3ForcePathStyle:    s3.S3ForcePathStyle,
	}

	clientOperationID, err := clientConn.ExportToS3(ctx, client, s3Settings)
	if err != nil {
		xlog.Error(ctx, "can't start export operation", zap.Error(err))
		return nil, nil, status.Errorf(codes.Unknown, "can't start export operation, dsn %s", dsn)
	}
	ctx = xlog.With(ctx, zap.String("ClientOperationID", clientOperationID))
	xlog.Info(ctx, "Export operation started")

	now := timestamppb.Now()
	backup := &types.Backup{
		ID:               types.GenerateObjectID(),
		ContainerID:      req.GetContainerId(),
		DatabaseName:     req.GetDatabaseName(),
		DatabaseEndpoint: req.GetDatabaseEndpoint(),
		S3Endpoint:       s3.Endpoint,
		S3Region:         s3.Region,
		S3Bucket:         s3.Bucket,
		S3PathPrefix:     destinationPrefix,
		Status:           types.BackupStateRunning,
		AuditInfo: &pb.AuditInfo{
			CreatedAt: now,
			Creator:   subject,
		},
		ScheduleID: scheduleId,
	}

	op := &types.TakeBackupOperation{
		ID:          types.GenerateObjectID(),
		BackupID:    backup.ID,
		ContainerID: req.ContainerId,
		State:       types.OperationStateRunning,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.GetDatabaseEndpoint(),
			DatabaseName: req.GetDatabaseName(),
		},
		SourcePaths:          req.GetSourcePaths(),
		SourcePathsToExclude: req.GetSourcePathsToExclude(),
		Audit: &pb.AuditInfo{
			CreatedAt: now,
			Creator:   subject,
		},
		YdbOperationId: clientOperationID,
		UpdatedAt:      now,
	}

	return backup, op, nil
}
