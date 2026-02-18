package types

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"

	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var (
	BackupStateUnknown   = pb.Backup_STATUS_UNSPECIFIED.String()
	BackupStatePending   = pb.Backup_PENDING.String()
	BackupStateRunning   = pb.Backup_RUNNING.String()
	BackupStateAvailable = pb.Backup_AVAILABLE.String()
	BackupStateError     = pb.Backup_ERROR.String()
	BackupStateCancelled = pb.Backup_CANCELLED.String()
	BackupStateDeleting  = pb.Backup_DELETING.String()
	BackupStateDeleted   = pb.Backup_DELETED.String()
)

func GenerateObjectID() string {
	return uuid.New().String()
}

func ParseObjectID(string string) (string, error) {
	parsed, err := uuid.Parse(string)
	if err != nil {
		return "", fmt.Errorf("invalid uuid: %w", err)
	}
	if parsed.Variant() != uuid.RFC4122 && parsed.Version() != 4 {
		return "", fmt.Errorf("string is not UUID4: %w", err)
	}
	return parsed.String(), nil
}

type Backup struct {
	ID                 string
	ContainerID        string
	DatabaseName       string
	DatabaseEndpoint   string
	S3Endpoint         string
	S3Region           string
	S3Bucket           string
	S3PathPrefix       string
	Status             string
	Message            string
	AuditInfo          *pb.AuditInfo
	Size               int64
	ScheduleID         *string
	ExpireAt           *time.Time
	SourcePaths        []string
	EncryptionSettings *pb.EncryptionSettings
}

func (o *Backup) SetLogFields(ctx context.Context) context.Context {
	if o == nil {
		return ctx
	}

	fields := make([]zap.Field, 0, 5)
	if o.ID != "" {
		fields = append(fields, zap.String(log_keys.BackupID, o.ID))
	}
	if o.ContainerID != "" {
		fields = append(fields, zap.String(log_keys.ContainerID, o.ContainerID))
	}
	if o.DatabaseName != "" {
		fields = append(fields, zap.String(log_keys.Database, o.DatabaseName))
	}
	if o.DatabaseEndpoint != "" {
		fields = append(fields, zap.String(log_keys.DatabaseEndpoint, o.DatabaseEndpoint))
	}
	if o.ScheduleID != nil {
		fields = append(fields, zap.String(log_keys.ScheduleID, *o.ScheduleID))
	}
	if len(fields) == 0 {
		return ctx
	}
	return xlog.With(ctx, fields...)
}

func (o *Backup) String() string {
	return fmt.Sprintf(
		"ID: %s, ContainerID: %s, DatabaseEndpoint: %s, DatabaseName: %s, Status %s",
		o.ID,
		o.ContainerID,
		o.DatabaseEndpoint,
		o.DatabaseName,
		o.Status,
	)
}

func (o *Backup) Proto() *pb.Backup {
	backup := &pb.Backup{
		Id:               o.ID,
		ContainerId:      o.ContainerID,
		DatabaseName:     o.DatabaseName,
		DatabaseEndpoint: o.DatabaseEndpoint,
		Location: &pb.S3Location{
			Endpoint:   o.S3Endpoint,
			Region:     o.S3Region,
			Bucket:     o.S3Bucket,
			PathPrefix: o.S3PathPrefix,
		},
		Audit:              o.AuditInfo,
		Size:               o.Size,
		Status:             pb.Backup_Status(pb.Backup_Status_value[o.Status]),
		Message:            o.Message,
		ExpireAt:           nil,
		SourcePaths:        o.SourcePaths,
		EncryptionSettings: o.EncryptionSettings,
	}
	if o.ScheduleID != nil {
		backup.ScheduleId = *o.ScheduleID
	}

	if o.ExpireAt != nil {
		backup.ExpireAt = timestamppb.New(*o.ExpireAt)
	}

	return backup
}

func (o *Backup) CanBeDeleted() bool {
	return o.Status == BackupStateAvailable || o.Status == BackupStateError || o.Status == BackupStateCancelled
}

func (o *Backup) SetCompletedAt(completedAt *timestamppb.Timestamp) {
	if o.AuditInfo != nil {
		o.AuditInfo.CompletedAt = completedAt
	} else {
		o.AuditInfo = &pb.AuditInfo{CompletedAt: completedAt}
	}
}

func (o *Backup) Empty() bool {
	return len(o.S3Endpoint) == 0 && len(o.S3Region) == 0 && len(o.S3Bucket) == 0 && len(o.S3PathPrefix) == 0
}
