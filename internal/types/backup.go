package types

import (
	"fmt"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"

	"github.com/google/uuid"
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
	ID               string
	ContainerID      string
	DatabaseName     string
	DatabaseEndpoint string
	S3Endpoint       string
	S3Region         string
	S3Bucket         string
	S3PathPrefix     string
	Status           string
	Message          string
	AuditInfo        *pb.AuditInfo
	Size             int64
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
	return &pb.Backup{
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
		Audit:    o.AuditInfo,
		Size:     o.Size,
		Status:   pb.Backup_Status(pb.Backup_Status_value[o.Status]),
		Message:  o.Message,
		ExpireAt: nil,
	}
}

func (o *Backup) CanBeDeleted() bool {
	return o.Status == BackupStateAvailable || o.Status == BackupStateError || o.Status == BackupStateCancelled
}
