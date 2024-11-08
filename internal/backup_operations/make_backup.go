package backup_operations

import (
	"context"
	"github.com/jonboulle/clockwork"
	"path"
	"regexp"
	"strings"
	"time"
	"ydbcp/internal/connectors/db"

	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	validEndpoint = regexp.MustCompile(`^(grpcs://|grpc://)?([A-Za-z0-9\-\.]+)(:[0-9]+)?$`)
)

type MakeBackupInternalRequest struct {
	ContainerID          string
	DatabaseEndpoint     string
	DatabaseName         string
	SourcePaths          []string
	SourcePathsToExclude []string
	ScheduleID           *string
	Ttl                  *time.Duration
}

func FromGRPCRequest(request *pb.MakeBackupRequest, scheduleID *string) MakeBackupInternalRequest {
	res := MakeBackupInternalRequest{
		ContainerID:          request.ContainerId,
		DatabaseEndpoint:     request.DatabaseEndpoint,
		DatabaseName:         request.DatabaseName,
		SourcePaths:          request.SourcePaths,
		SourcePathsToExclude: request.SourcePathsToExclude,
		ScheduleID:           scheduleID,
	}
	if ttl := request.Ttl.AsDuration(); request.Ttl != nil {
		res.Ttl = &ttl
	}
	return res
}

func FromTBWROperation(tbwr *types.TakeBackupWithRetryOperation) MakeBackupInternalRequest {
	return MakeBackupInternalRequest{
		ContainerID:          tbwr.ContainerID,
		DatabaseEndpoint:     tbwr.YdbConnectionParams.Endpoint,
		DatabaseName:         tbwr.YdbConnectionParams.DatabaseName,
		SourcePaths:          tbwr.SourcePaths,
		SourcePathsToExclude: tbwr.SourcePathsToExclude,
		ScheduleID:           tbwr.ScheduleID,
		Ttl:                  tbwr.Ttl,
	}
}

func SafePathJoin(base string, relPath ...string) (fullPath string, ok bool) {
	paths := append([]string{base}, relPath...)
	fullPath = path.Join(paths...)
	if strings.HasPrefix(fullPath, base+"/") || fullPath == base {
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
	req MakeBackupInternalRequest,
	subject string,
	clock clockwork.Clock,
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
		clock.Now().Format(types.BackupTimestampFormat),
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

	pathsForExport, err := clientConn.PreparePathsForExport(ctx, client, sourcePaths, req.SourcePathsToExclude)
	if err != nil {
		xlog.Error(
			ctx,
			"error preparing paths for export",
			zap.Strings("sourcePaths", req.SourcePaths),
			zap.String("scheduleID", db.StringOrEmpty(req.ScheduleID)),
			zap.Error(err),
		)
		return nil, nil, status.Errorf(codes.Unknown, "error preparing paths for export, dsn %s", dsn)
	}

	if len(pathsForExport) == 0 {
		xlog.Error(
			ctx,
			"empty list of paths for export",
			zap.String("scheduleID", db.StringOrEmpty(req.ScheduleID)),
		)
		return nil, nil, status.Error(codes.FailedPrecondition, "empty list of paths for export")
	}

	s3Settings := types.ExportSettings{
		Endpoint:          s3.Endpoint,
		Region:            s3.Region,
		Bucket:            s3.Bucket,
		AccessKey:         accessKey,
		SecretKey:         secretKey,
		Description:       "ydbcp backup", // TODO: the description shoud be better
		NumberOfRetries:   10,             // TODO: get it from configuration
		SourcePaths:       pathsForExport,
		DestinationPrefix: destinationPrefix,
		S3ForcePathStyle:  s3.S3ForcePathStyle,
	}

	clientOperationID, err := clientConn.ExportToS3(ctx, client, s3Settings)
	if err != nil {
		xlog.Error(ctx, "can't start export operation", zap.Error(err))
		return nil, nil, status.Errorf(codes.Unknown, "can't start export operation, dsn %s", dsn)
	}
	ctx = xlog.With(ctx, zap.String("ClientOperationID", clientOperationID))
	xlog.Info(ctx, "Export operation started")

	var expireAt *time.Time
	if req.Ttl != nil {
		expireAt = new(time.Time)
		*expireAt = clock.Now().Add(*req.Ttl)
	}

	now := timestamppb.New(clock.Now())
	backup := &types.Backup{
		ID:               types.GenerateObjectID(),
		ContainerID:      req.ContainerID,
		DatabaseName:     req.DatabaseName,
		DatabaseEndpoint: req.DatabaseEndpoint,
		S3Endpoint:       s3.Endpoint,
		S3Region:         s3.Region,
		S3Bucket:         s3.Bucket,
		S3PathPrefix:     destinationPrefix,
		Status:           types.BackupStateRunning,
		AuditInfo: &pb.AuditInfo{
			CreatedAt: now,
			Creator:   subject,
		},
		ScheduleID:  req.ScheduleID,
		ExpireAt:    expireAt,
		SourcePaths: pathsForExport,
	}

	op := &types.TakeBackupOperation{
		ID:          types.GenerateObjectID(),
		BackupID:    backup.ID,
		ContainerID: req.ContainerID,
		State:       types.OperationStateRunning,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.DatabaseEndpoint,
			DatabaseName: req.DatabaseName,
		},
		SourcePaths:          req.SourcePaths,
		SourcePathsToExclude: req.SourcePathsToExclude,
		Audit: &pb.AuditInfo{
			CreatedAt: now,
			Creator:   subject,
		},
		YdbOperationId: clientOperationID,
		UpdatedAt:      now,
	}

	return backup, op, nil
}
