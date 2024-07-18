package types

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"strings"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"go.uber.org/zap"
	"time"

	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto"
)

type ObjectID uuid.UUID

func MustObjectIDFromBytes(b [16]byte) ObjectID {
	return ObjectID(uuid.Must(uuid.FromBytes(b[:])))
}

func (bid ObjectID) Bytes() [16]byte {
	return bid
}

func (bid ObjectID) String() string {
	return (uuid.UUID)(bid).String()
}

// MarshalText makes marshalling in log prettier.
func (bid ObjectID) MarshalText() ([]byte, error) {
	return (uuid.UUID)(bid).MarshalText()
}

func GenerateObjectID() ObjectID {
	return ObjectID(uuid.New())
}

func ParseObjectId(objectId string) (ObjectID, error) {
	parsed, err := uuid.Parse(objectId)
	if err != nil {
		return ObjectID{}, fmt.Errorf("Invalid uuid: %w", err)
	}
	if parsed.Variant() != uuid.RFC4122 && parsed.Version() != 4 {
		return ObjectID{}, fmt.Errorf("ObjectId is not UUID4: %w", err)
	}
	return ObjectID(parsed), nil
}

type Backup struct {
	ID           ObjectID
	ContainerID  string
	DatabaseName string
	S3Endpoint   string
	S3Region     string
	S3Bucket     string
	S3PathPrefix string
	Status       string
}

func (o *Backup) String() string {
	return fmt.Sprintf(
		"ID: %s, ContainerID: %s, DatabaseName: %s, Status %s",
		o.ID,
		o.ContainerID,
		o.DatabaseName,
		o.Status,
	)
}

func (o *Backup) Proto() *pb.Backup {
	return &pb.Backup{
		Id:           o.ID.String(),
		ContainerId:  o.ContainerID,
		DatabaseName: o.DatabaseName,
		Location:     nil,
		Audit:        nil,
		Size:         0,
		Status:       pb.Backup_Status(pb.Backup_Status_value[o.Status]),
		Message:      "",
		ExpireAt:     nil,
	}
}

type OperationType string
type OperationState string

type Operation interface {
	GetId() ObjectID
	SetId(id ObjectID)
	GetType() OperationType
	SetType(t OperationType)
	GetState() OperationState
	SetState(s OperationState)
	GetMessage() string
	SetMessage(m string)
	Proto() *pb.Operation
}

type TakeBackupOperation struct {
	Id                  ObjectID
	ContainerID         string
	BackupId            ObjectID
	State               OperationState
	Message             string
	YdbConnectionParams YdbConnectionParams
	YdbOperationId      string
	SourcePaths         []string
	SourcePathToExclude []string
}

func (o *TakeBackupOperation) GetId() ObjectID {
	return o.Id
}
func (o *TakeBackupOperation) SetId(id ObjectID) {
	o.Id = id
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

func (o *TakeBackupOperation) Proto() *pb.Operation {
	return &pb.Operation{
		Id:                   o.Id.String(),
		ContainerId:          o.ContainerID,
		Type:                 string(OperationTypeTB),
		DatabaseName:         o.YdbConnectionParams.DatabaseName,
		YdbServerOperationId: o.YdbOperationId,
		BackupId:             o.BackupId.String(),
		SourcePaths:          o.SourcePaths,
		SourcePathsToExclude: o.SourcePathToExclude,
		RestorePaths:         nil,
		Audit:                nil,
		Status:               o.State.Enum(),
		Message:              o.Message,
	}
}

type RestoreBackupOperation struct {
	Id                  ObjectID
	ContainerID         string
	BackupId            ObjectID
	State               OperationState
	Message             string
	YdbConnectionParams YdbConnectionParams
	YdbOperationId      string
	DestinationPaths    []string
	CreatedAt           time.Time
}

func (o *RestoreBackupOperation) GetId() ObjectID {
	return o.Id
}
func (o *RestoreBackupOperation) SetId(id ObjectID) {
	o.Id = id
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

func (o *RestoreBackupOperation) Proto() *pb.Operation {
	return &pb.Operation{
		Id:                   o.Id.String(),
		ContainerId:          o.ContainerID,
		Type:                 string(OperationTypeTB),
		DatabaseName:         o.YdbConnectionParams.DatabaseName,
		YdbServerOperationId: o.YdbOperationId,
		BackupId:             o.BackupId.String(),
		SourcePaths:          nil,
		SourcePathsToExclude: nil,
		RestorePaths:         o.DestinationPaths,
		Audit: &pb.AuditInfo{
			Creator:     "",
			CreatedAt:   timestamppb.New(o.CreatedAt),
			CompletedAt: nil,
		},
		Status:  o.State.Enum(),
		Message: o.Message,
	}
}

type GenericOperation struct {
	Id          ObjectID
	ContainerID string
	Type        OperationType
	State       OperationState
	Message     string
}

func (o *GenericOperation) GetId() ObjectID {
	return o.Id
}
func (o *GenericOperation) SetId(id ObjectID) {
	o.Id = id
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
func (o *GenericOperation) Proto() *pb.Operation {
	log.Fatalf("Converting GenericOperation to Proto: %s", o.Id)
	return nil
}

var (
	OperationStateUnknown    = OperationState(pb.Operation_STATUS_UNSPECIFIED.String())
	OperationStatePending    = OperationState(pb.Operation_PENDING.String())
	OperationStateDone       = OperationState(pb.Operation_DONE.String())
	OperationStateError      = OperationState(pb.Operation_ERROR.String())
	OperationStateCancelling = OperationState(pb.Operation_CANCELLING.String())
	OperationStateCancelled  = OperationState(pb.Operation_CANCELED.String())
)

const (
	OperationTypeTB = OperationType("TB")
	OperationTypeRB = OperationType("RB")

	BackupStateUnknown   = "Unknown"
	BackupStatePending   = "Pending"
	BackupStateAvailable = "Available"
	BackupStateError     = "Error"
	BackupStateCancelled = "Cancelled"
)

func OperationToString(o Operation) string {
	return fmt.Sprintf(
		"Operation, id %s, type %s, state %s",
		o.GetId().String(),
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
	return o.GetState() == OperationStatePending || o.GetState() == OperationStateCancelling
}

func IssuesToString(issues []*Ydb_Issue.IssueMessage) string {
	str := make([]string, len(issues))
	for i, v := range issues {
		str[i] = v.String()
	}
	return strings.Join(str, ", ")
}

func GetYdbConnectionParams(dbname string) YdbConnectionParams {
	return YdbConnectionParams{
		Endpoint:     "grpc://localhost:2136", // TODO
		DatabaseName: dbname,
	}
}

type YdbConnectionParams struct {
	Endpoint     string
	DatabaseName string
	// TODO: add auth params
}

func MakeYdbConnectionString(params YdbConnectionParams) string {
	return params.Endpoint + params.DatabaseName
}

type OperationHandler func(context.Context, Operation) error
