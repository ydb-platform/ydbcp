package types

import (
	"fmt"
	"github.com/google/uuid"
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
}

type OperationType string
type OperationState string
type Operation struct {
	Id      ObjectID
	Type    OperationType
	State   string
	Message string
}

const (
	StateUnknown    = "Unknown"
	StatePending    = "Pending"
	StateDone       = "Done"
	StateError      = "Error"
	StateCancelling = "Cancelling"
	StateCancelled  = "Cancelled"
)

func (o Operation) String() string {
	return fmt.Sprintf(
		"Operation, id %s, type %s, state %s",
		o.Id.String(),
		o.Type,
		o.State,
	)
}

func (o Operation) IsActive() bool {
	return o.State == StatePending || o.State == StateCancelling
}
