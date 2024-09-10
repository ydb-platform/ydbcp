package types

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"go.uber.org/zap"
	"log"
	"strings"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

type OperationType string
type OperationState string

type Operation interface {
	GetID() string
	SetID(id string)
	GetContainerID() string
	GetType() OperationType
	SetType(t OperationType)
	GetState() OperationState
	SetState(s OperationState)
	GetMessage() string
	SetMessage(m string)
	GetAudit() *pb.AuditInfo
	Copy() Operation
	Proto() *pb.Operation
}

type TakeBackupOperation struct {
	ID                   string
	ContainerID          string
	BackupID             string
	State                OperationState
	Message              string
	YdbConnectionParams  YdbConnectionParams
	YdbOperationId       string
	SourcePaths          []string
	SourcePathsToExclude []string
	Audit                *pb.AuditInfo
}

func (o *TakeBackupOperation) GetID() string {
	return o.ID
}
func (o *TakeBackupOperation) SetID(id string) {
	o.ID = id
}
func (o *TakeBackupOperation) GetContainerID() string {
	return o.ContainerID
}
func (o *TakeBackupOperation) GetType() OperationType {
	return OperationTypeTB
}
func (o *TakeBackupOperation) SetType(_ OperationType) {
}
func (o *TakeBackupOperation) GetState() OperationState {
	return o.State
}
func (o *TakeBackupOperation) SetState(s OperationState) {
	o.State = s
}
func (o *TakeBackupOperation) GetMessage() string {
	return o.Message
}
func (o *TakeBackupOperation) SetMessage(m string) {
	o.Message = m
}
func (o *TakeBackupOperation) GetAudit() *pb.AuditInfo {
	return o.Audit
}
func (o *TakeBackupOperation) Copy() Operation {
	copy := *o
	return &copy
}

func (o *TakeBackupOperation) Proto() *pb.Operation {
	return &pb.Operation{
		Id:                   o.ID,
		ContainerId:          o.ContainerID,
		Type:                 string(OperationTypeTB),
		DatabaseName:         o.YdbConnectionParams.DatabaseName,
		DatabaseEndpoint:     o.YdbConnectionParams.Endpoint,
		YdbServerOperationId: o.YdbOperationId,
		BackupId:             o.BackupID,
		SourcePaths:          o.SourcePaths,
		SourcePathsToExclude: o.SourcePathsToExclude,
		RestorePaths:         nil,
		Audit:                o.Audit,
		Status:               o.State.Enum(),
		Message:              o.Message,
	}
}

type RestoreBackupOperation struct {
	ID                  string
	ContainerID         string
	BackupId            string
	State               OperationState
	Message             string
	YdbConnectionParams YdbConnectionParams
	YdbOperationId      string
	// TODO: this field is not used, because DestinationPaths
	// will be constructed from SourcePaths and DestinationPrefix
	DestinationPaths  []string
	SourcePaths       []string
	DestinationPrefix string
	Audit             *pb.AuditInfo
}

func (o *RestoreBackupOperation) GetID() string {
	return o.ID
}
func (o *RestoreBackupOperation) SetID(id string) {
	o.ID = id
}
func (o *RestoreBackupOperation) GetContainerID() string {
	return o.ContainerID
}
func (o *RestoreBackupOperation) GetType() OperationType {
	return OperationTypeRB
}
func (o *RestoreBackupOperation) SetType(_ OperationType) {
}
func (o *RestoreBackupOperation) GetState() OperationState {
	return o.State
}
func (o *RestoreBackupOperation) SetState(s OperationState) {
	o.State = s
}
func (o *RestoreBackupOperation) GetMessage() string {
	return o.Message
}
func (o *RestoreBackupOperation) SetMessage(m string) {
	o.Message = m
}
func (o *RestoreBackupOperation) GetAudit() *pb.AuditInfo {
	return o.Audit
}
func (o *RestoreBackupOperation) Copy() Operation {
	copy := *o
	return &copy
}

func (o *RestoreBackupOperation) Proto() *pb.Operation {
	return &pb.Operation{
		Id:                   o.ID,
		ContainerId:          o.ContainerID,
		Type:                 string(OperationTypeRB),
		DatabaseName:         o.YdbConnectionParams.DatabaseName,
		DatabaseEndpoint:     o.YdbConnectionParams.Endpoint,
		YdbServerOperationId: o.YdbOperationId,
		BackupId:             o.BackupId,
		SourcePaths:          o.SourcePaths,
		SourcePathsToExclude: nil,
		RestorePaths:         o.DestinationPaths,
		Audit:                o.Audit,
		Status:               o.State.Enum(),
		Message:              o.Message,
	}
}

type DeleteBackupOperation struct {
	ID                  string
	ContainerID         string
	YdbConnectionParams YdbConnectionParams
	BackupID            string
	State               OperationState
	Message             string
	PathPrefix          string
	Audit               *pb.AuditInfo
}

func (o *DeleteBackupOperation) GetID() string {
	return o.ID
}
func (o *DeleteBackupOperation) SetID(id string) {
	o.ID = id
}
func (o *DeleteBackupOperation) GetContainerID() string {
	return o.ContainerID
}
func (o *DeleteBackupOperation) GetType() OperationType {
	return OperationTypeDB
}
func (o *DeleteBackupOperation) SetType(_ OperationType) {
}
func (o *DeleteBackupOperation) GetState() OperationState {
	return o.State
}
func (o *DeleteBackupOperation) SetState(s OperationState) {
	o.State = s
}
func (o *DeleteBackupOperation) GetMessage() string {
	return o.Message
}
func (o *DeleteBackupOperation) SetMessage(m string) {
	o.Message = m
}
func (o *DeleteBackupOperation) GetAudit() *pb.AuditInfo {
	return o.Audit
}
func (o *DeleteBackupOperation) Copy() Operation {
	copy := *o
	return &copy
}

func (o *DeleteBackupOperation) Proto() *pb.Operation {
	return &pb.Operation{
		Id:                   o.ID,
		ContainerId:          o.ContainerID,
		Type:                 string(OperationTypeDB),
		DatabaseName:         o.YdbConnectionParams.DatabaseName,
		DatabaseEndpoint:     o.YdbConnectionParams.Endpoint,
		YdbServerOperationId: "",
		BackupId:             o.BackupID,
		SourcePaths:          []string{o.PathPrefix},
		SourcePathsToExclude: nil,
		RestorePaths:         nil,
		Audit:                o.Audit,
		Status:               o.State.Enum(),
		Message:              o.Message,
	}
}

type GenericOperation struct {
	ID          string
	ContainerID string
	Type        OperationType
	State       OperationState
	Message     string
}

func (o *GenericOperation) GetID() string {
	return o.ID
}
func (o *GenericOperation) SetID(id string) {
	o.ID = id
}
func (o *GenericOperation) GetContainerID() string {
	return o.ContainerID
}
func (o *GenericOperation) GetType() OperationType {
	return o.Type
}
func (o *GenericOperation) SetType(t OperationType) {
	o.Type = t
}
func (o *GenericOperation) GetState() OperationState {
	return o.State
}
func (o *GenericOperation) SetState(s OperationState) {
	o.State = s
}
func (o *GenericOperation) GetMessage() string {
	return o.Message
}
func (o *GenericOperation) SetMessage(m string) {
	o.Message = m
}
func (o *GenericOperation) GetAudit() *pb.AuditInfo {
	return nil
}
func (o *GenericOperation) Copy() Operation {
	copy := *o
	return &copy
}

func (o *GenericOperation) Proto() *pb.Operation {
	log.Fatalf("Converting GenericOperation to Proto: %s", o.ID)
	return nil
}

var (
	OperationStateUnknown         = OperationState(pb.Operation_STATUS_UNSPECIFIED.String())
	OperationStatePending         = OperationState(pb.Operation_PENDING.String())
	OperationStateRunning         = OperationState(pb.Operation_RUNNING.String())
	OperationStateDone            = OperationState(pb.Operation_DONE.String())
	OperationStateError           = OperationState(pb.Operation_ERROR.String())
	OperationStateCancelling      = OperationState(pb.Operation_CANCELLING.String())
	OperationStateCancelled       = OperationState(pb.Operation_CANCELED.String())
	OperationStateStartCancelling = OperationState(pb.Operation_START_CANCELLING.String())

	BackupStateUnknown   = pb.Backup_STATUS_UNSPECIFIED.String()
	BackupStatePending   = pb.Backup_PENDING.String()
	BackupStateRunning   = pb.Backup_RUNNING.String()
	BackupStateAvailable = pb.Backup_AVAILABLE.String()
	BackupStateError     = pb.Backup_ERROR.String()
	BackupStateCancelled = pb.Backup_CANCELLED.String()
	BackupStateDeleting  = pb.Backup_DELETING.String()
	BackupStateDeleted   = pb.Backup_DELETED.String()
)

const (
	OperationTypeTB       = OperationType("TB")
	OperationTypeRB       = OperationType("RB")
	OperationTypeDB       = OperationType("DB")
	BackupTimestampFormat = "20060102_150405"
	S3ForcePathStyle      = true
)

func OperationToString(o Operation) string {
	return fmt.Sprintf(
		"Operation, id %s, type %s, state %s",
		o.GetID(),
		o.GetType(),
		o.GetState(),
	)
}

func (o OperationState) Enum() pb.Operation_Status {
	val, ok := pb.Operation_Status_value[string(o)]
	if !ok {
		xlog.Error(
			context.Background(), //TODO
			"Can't convert OperationState",
			zap.String("OperationState", string(o)),
		)
		panic("can't convert OperationState")
	}
	return pb.Operation_Status(val)
}

func (o OperationState) String() string {
	return string(o)
}

func IsActive(o Operation) bool {
	return o.GetState() == OperationStatePending ||
		o.GetState() == OperationStateRunning ||
		o.GetState() == OperationStateStartCancelling ||
		o.GetState() == OperationStateCancelling
}

func IssuesToString(issues []*Ydb_Issue.IssueMessage) string {
	str := make([]string, len(issues))
	for i, v := range issues {
		str[i] = v.String()
	}
	return strings.Join(str, ", ")
}

type OperationHandler func(context.Context, Operation) error
