package backup_operations

import (
	"context"
	"errors"
	"fmt"
	"github.com/jonboulle/clockwork"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"path"
	"regexp"
	"strings"
	"time"
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
	ParentOperationID    *string
}

func FromBackupSchedule(schedule *types.BackupSchedule) MakeBackupInternalRequest {
	res := MakeBackupInternalRequest{
		ContainerID:          schedule.ContainerID,
		DatabaseEndpoint:     schedule.DatabaseEndpoint,
		DatabaseName:         schedule.DatabaseName,
		SourcePaths:          schedule.SourcePaths,
		SourcePathsToExclude: schedule.SourcePathsToExclude,
		ScheduleID:           &schedule.ID,
	}
	if ttl := schedule.ScheduleSettings.Ttl.AsDuration(); schedule.ScheduleSettings.Ttl != nil {
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
		ParentOperationID:    &tbwr.ID,
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

func OpenConnAndValidateSourcePaths(ctx context.Context, req MakeBackupInternalRequest, clientConn client.ClientConnector) error {
	clientConnectionParams := types.YdbConnectionParams{
		Endpoint:     req.DatabaseEndpoint,
		DatabaseName: req.DatabaseName,
	}
	dsn := types.MakeYdbConnectionString(clientConnectionParams)
	ctx = xlog.With(ctx, zap.String("ClientDSN", dsn))
	client, err := clientConn.Open(ctx, dsn)
	if err != nil {
		xlog.Error(ctx, "can't open client connection", zap.Error(err))
		return status.Errorf(codes.Unknown, "can't open client connection, dsn %s", dsn)
	}
	defer func() {
		if err := clientConn.Close(ctx, client); err != nil {
			xlog.Error(ctx, "can't close client connection", zap.Error(err))
		}
	}()
	_, err = ValidateSourcePaths(ctx, req, clientConn, client, dsn)
	var empty *EmptyDatabaseError
	if errors.As(err, &empty) {
		return nil
	} else {
		return err
	}
}

func ValidateSourcePaths(ctx context.Context, req MakeBackupInternalRequest, clientConn client.ClientConnector, client *ydb.Driver, dsn string) ([]string, error) {
	if req.ScheduleID != nil {
		ctx = xlog.With(ctx, zap.String("ScheduleID", *req.ScheduleID))
	}
	sourcePaths := make([]string, 0, len(req.SourcePaths))
	for _, p := range req.SourcePaths {
		fullPath, ok := SafePathJoin(req.DatabaseName, p)
		if !ok {
			xlog.Error(ctx, "incorrect source path", zap.String("path", p))
			return nil, status.Errorf(codes.InvalidArgument, "incorrect source path %s", p)
		}
		sourcePaths = append(sourcePaths, fullPath)
	}

	pathsForExport, err := clientConn.PreparePathsForExport(ctx, client, sourcePaths, req.SourcePathsToExclude)
	if err != nil {
		xlog.Error(ctx, "error preparing paths for export", zap.Error(err))
		return nil, status.Errorf(codes.Unknown, "error preparing paths for export, dsn %s", dsn)
	}

	if len(pathsForExport) == 0 {
		xlog.Error(ctx, "empty list of paths for export")
		return nil, NewEmptyDatabaseError(codes.FailedPrecondition, "empty list of paths for export")
	}
	return pathsForExport, nil
}

type ClientConnectionError struct {
	err error
}

func NewClientConnectionError(code codes.Code, message string) *ClientConnectionError {
	return &ClientConnectionError{err: status.Errorf(code, message)}
}

func (e ClientConnectionError) Error() string {
	return e.err.Error()
}

type EmptyDatabaseError struct {
	err error
}

func (e EmptyDatabaseError) Error() string {
	return e.err.Error()
}

func NewEmptyDatabaseError(code codes.Code, message string) *EmptyDatabaseError {
	return &EmptyDatabaseError{err: status.Errorf(code, message)}
}

func ErrToStatus(err error) error {
	var ce *ClientConnectionError
	var ee *EmptyDatabaseError

	if errors.As(err, &ce) {
		return ce.err
	}
	if errors.As(err, &ee) {
		return ee.err
	}
	return err
}

func CreateS3DestinationPrefix(databaseName string, s3 config.S3Config, clock clockwork.Clock) string {
	dbNamePath := strings.Replace(databaseName, "/", "_", -1) // TODO: checking user input
	dbNamePath = strings.Trim(dbNamePath, "_")
	return path.Join(
		s3.PathPrefix,
		dbNamePath,
		clock.Now().Format(types.BackupTimestampFormat),
	)
}

func CreateEmptyBackup(
	req MakeBackupInternalRequest,
	clock clockwork.Clock,
) (*types.Backup, *types.TakeBackupOperation) {
	var expireAt *time.Time
	if req.Ttl != nil {
		expireAt = new(time.Time)
		*expireAt = clock.Now().Add(*req.Ttl)
	}

	now := timestamppb.New(clock.Now())
	backup := &types.Backup{
		ID:               types.GenerateObjectID(),
		Message:          "empty backup",
		ContainerID:      req.ContainerID,
		DatabaseName:     req.DatabaseName,
		DatabaseEndpoint: req.DatabaseEndpoint,
		Status:           types.BackupStateAvailable,
		AuditInfo: &pb.AuditInfo{
			CreatedAt:   now,
			CompletedAt: now,
			Creator:     types.OperationCreatorName,
		},
		ScheduleID:  req.ScheduleID,
		ExpireAt:    expireAt,
		SourcePaths: []string{},
	}

	op := &types.TakeBackupOperation{
		ID:          types.GenerateObjectID(),
		BackupID:    backup.ID,
		ContainerID: req.ContainerID,
		State:       types.OperationStateDone,
		YdbConnectionParams: types.YdbConnectionParams{
			Endpoint:     req.DatabaseEndpoint,
			DatabaseName: req.DatabaseName,
		},
		SourcePaths:          req.SourcePaths,
		SourcePathsToExclude: req.SourcePathsToExclude,
		Audit: &pb.AuditInfo{
			CreatedAt:   now,
			CompletedAt: now,
			Creator:     types.OperationCreatorName,
		},
		YdbOperationId:    "",
		UpdatedAt:         now,
		ParentOperationID: req.ParentOperationID,
	}

	return backup, op
}

func IsEmptyBackup(backup *types.Backup) bool {
	return backup.Size == 0 && backup.S3Endpoint == ""
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
	if req.ScheduleID != nil {
		ctx = xlog.With(ctx, zap.String("ScheduleID", *req.ScheduleID))
	}
	if !IsAllowedEndpoint(req.DatabaseEndpoint, allowedEndpointDomains, allowInsecureEndpoint) {
		xlog.Error(
			ctx,
			"endpoint of database is invalid or not allowed",
			zap.String("DatabaseEndpoint", req.DatabaseEndpoint),
		)
		return nil, nil, NewClientConnectionError(
			codes.FailedPrecondition,
			fmt.Sprintf("endpoint of database is invalid or not allowed, endpoint %s", req.DatabaseEndpoint),
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
		return nil, nil, NewClientConnectionError(codes.Unknown, fmt.Sprintf("can't open client connection, dsn %s", dsn))
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

	destinationPrefix := CreateS3DestinationPrefix(req.DatabaseName, s3, clock)
	ctx = xlog.With(ctx, zap.String("S3DestinationPrefix", destinationPrefix))

	pathsForExport, err := ValidateSourcePaths(ctx, req, clientConn, client, dsn)

	if err != nil {
		return nil, nil, err
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
		YdbOperationId:    clientOperationID,
		UpdatedAt:         now,
		ParentOperationID: req.ParentOperationID,
	}

	return backup, op, nil
}
