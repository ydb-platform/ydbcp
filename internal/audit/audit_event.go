package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"os"
	"time"
	"ydbcp/internal/util/xlog"
)

var EventsDestination string

type Event struct { //flat event struct for everything
	Resource     string
	Action       Action
	Component    string
	MethodName   string
	GRPCRequest  proto.Message
	AuthRequest  proto.Message
	AuthResponse proto.Message
	Status       *status.Status
	Timestamp    time.Time
}

type EventJson struct {
	Destination string
	Event       *Event
	Type        string
}

func marshalProtoMessage(msg proto.Message) json.RawMessage {
	if msg == nil {
		return nil
	}
	b, err := protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   true,
	}.Marshal(msg)
	if err != nil {
		return json.RawMessage(`"error marshaling proto message"`)
	}
	return b
}

func (e *Event) MarshalJSON() ([]byte, error) {
	type Alias Event
	return json.Marshal(&struct {
		GRPCRequest  json.RawMessage `json:"grpc_request,omitempty"`
		AuthRequest  json.RawMessage `json:"auth_request,omitempty"`
		AuthResponse json.RawMessage `json:"auth_response,omitempty"`
		Status       json.RawMessage `json:"status,omitempty"`
		Timestamp    string          `json:"timestamp"`
		*Alias
	}{
		GRPCRequest:  marshalProtoMessage(e.GRPCRequest),
		AuthRequest:  marshalProtoMessage(e.AuthRequest),
		AuthResponse: marshalProtoMessage(e.AuthResponse),
		Status:       marshalProtoMessage(e.Status.Proto()),
		Timestamp:    e.Timestamp.Format(time.RFC3339Nano),
		Alias:        (*Alias)(e),
	})
}

func (ej *EventJson) MarshalJSON() ([]byte, error) {
	type Alias EventJson
	return json.Marshal(&struct {
		Event *Event `json:"event,omitempty"`
		*Alias
	}{
		Event: ej.Event,
		Alias: (*Alias)(ej),
	})
}
func getGRPCStatus(err error) *status.Status {
	if err == nil {
		return status.New(codes.OK, "")
	}
	return status.Convert(err)
}

func GRPCCallAuditEvent(ctx context.Context, methodName string, req proto.Message, err error) *Event {
	return &Event{
		Resource:    "testresource",
		Component:   "grpc_api",
		MethodName:  methodName,
		GRPCRequest: req,
		Action:      FromMethodName(ctx, methodName),
		Status:      getGRPCStatus(err),
	}
}

func AuthCallAuditEvent(req proto.Message, resp proto.Message, err error) *Event {
	return &Event{
		Component:    "iam_auth",
		AuthRequest:  req,
		AuthResponse: resp,
		Status:       getGRPCStatus(err),
	}
}

func ReportGRPCCall(ctx context.Context, req proto.Message, methodName string, err error) {
	event := GRPCCallAuditEvent(ctx, methodName, req, err)
	ReportAuditEvent(ctx, event)
}

func ReportAuditEvent(ctx context.Context, event *Event) {
	event.Timestamp = time.Now()

	jsonData, err := json.Marshal(&EventJson{
		Destination: EventsDestination,
		Event:       event,
		Type:        "ydbcp-audit",
	})
	if err != nil {
		xlog.Error(ctx, "error reporting audit event", zap.Error(err))
		return
	}

	_, err = fmt.Fprintln(os.Stderr, string(jsonData))
	if err != nil {
		xlog.Error(ctx, "error reporting audit event", zap.Error(err))
	}
}
