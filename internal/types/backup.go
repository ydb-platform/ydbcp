package types

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"strings"
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

type Backup struct {
	Id          ObjectID
	OperationId ObjectID
	Status      string
}

func (backup *Backup) Proto() *pb.Backup {
	return &pb.Backup{
		Id:           backup.Id.String(),
		ContainerId:  "",
		DatabaseName: "",
		Location:     nil,
		Audit:        nil,
		Size:         0,
		Status:       0,
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
	GetState() string
	SetState(s OperationState)
	GetMessage() string
	SetMessage(m string)
}

type TakeBackupOperation struct {
	Id                  ObjectID
	BackupId            ObjectID
	Type                OperationType
	State               string
	Message             string
	YdbConnectionParams YdbConnectionParams
	YdbOperationId      string
}

func (o *TakeBackupOperation) GetId() ObjectID {
	return o.Id
}
func (o *TakeBackupOperation) SetId(id ObjectID) {
	o.Id = id
}
func (o *TakeBackupOperation) GetType() OperationType {
	return o.Type
}
func (o *TakeBackupOperation) SetType(t OperationType) {
	o.Type = t
}
func (o *TakeBackupOperation) GetState() string {
	return o.State
}
func (o *TakeBackupOperation) SetState(s OperationState) {
	o.State = string(s)
}
func (o *TakeBackupOperation) GetMessage() string {
	return o.Message
}
func (o *TakeBackupOperation) SetMessage(m string) {
	o.Message = m
}

type GenericOperation struct {
	Id      ObjectID
	Type    OperationType
	State   string
	Message string
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
func (o *GenericOperation) GetState() string {
	return o.State
}
func (o *GenericOperation) SetState(s OperationState) {
	o.State = string(s)
}
func (o *GenericOperation) GetMessage() string {
	return o.Message
}
func (o *GenericOperation) SetMessage(m string) {
	o.Message = m
}

const (
	OperationStateUnknown    = "Unknown"
	OperationStatePending    = "Pending"
	OperationStateDone       = "Done"
	OperationStateError      = "Error"
	OperationStateCancelling = "Cancelling"
	OperationStateCancelled  = "Cancelled"

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
		Endpoint:     "grpc://localhost:2136",
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
