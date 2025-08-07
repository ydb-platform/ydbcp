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
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/util/xlog"
)

var EventsDestination string

type Event struct { //flat event struct for everything
	ID           string
	Action       Action
	Component    string
	MethodName   string
	ContainerID  string
	Subject      string
	Token        string
	Resource     Resource
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
	return json.Marshal(&struct {
		ID           string          `json:"id"`
		Service      string          `json:"service"`
		SpecVersion  string          `json:"specversion"`
		Action       Action          `json:"action"`
		Resource     Resource        `json:"resource"`
		Component    string          `json:"component"`
		MethodName   string          `json:"method_name,omitempty"`
		ContainerID  string          `json:"container_id"`
		Subject      string          `json:"subject"`
		GRPCRequest  json.RawMessage `json:"grpc_request,omitempty"`
		AuthRequest  json.RawMessage `json:"auth_request,omitempty"`
		AuthResponse json.RawMessage `json:"auth_response,omitempty"`
		Status       json.RawMessage `json:"status,omitempty"`
		Timestamp    string          `json:"timestamp"`
	}{
		ID:           e.ID,
		Service:      "ydbcp",
		SpecVersion:  "1.0",
		Action:       e.Action,
		Resource:     e.Resource,
		Component:    e.Component,
		MethodName:   e.MethodName,
		ContainerID:  e.ContainerID,
		Subject:      e.Subject,
		GRPCRequest:  marshalProtoMessage(e.GRPCRequest),
		AuthRequest:  marshalProtoMessage(e.AuthRequest),
		AuthResponse: marshalProtoMessage(e.AuthResponse),
		Status:       marshalProtoMessage(e.Status.Proto()),
		Timestamp:    e.Timestamp.Format(time.RFC3339Nano),
	})
}

func (ej *EventJson) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Destination string `json:"destination,omitempty"`
		Event       *Event `json:"event"`
		Type        string `json:"type"`
	}{
		Destination: ej.Destination,
		Event:       ej.Event,
		Type:        ej.Type,
	})
}

func getGRPCStatus(err error) *status.Status {
	if err == nil {
		return status.New(codes.OK, "Success")
	}
	return status.Convert(err)
}

func GRPCCallAuditEvent(ctx context.Context, methodName string, req proto.Message, containerID string, subject string, err error) *Event {
	return &Event{
		ID:          grpcinfo.GetRequestID(ctx),
		Component:   "grpc_api",
		MethodName:  methodName,
		GRPCRequest: req,
		ContainerID: containerID,
		Subject:     subject,
		Action:      ActionFromMethodName(ctx, methodName),
		Resource:    ResourceFromMethodName(ctx, methodName),
		Status:      getGRPCStatus(err),
	}
}

func AuthCallAuditEvent(ctx context.Context, req proto.Message, resp proto.Message, subject string, err error) *Event {
	return &Event{
		ID:           grpcinfo.GetRequestID(ctx),
		Component:    "iam_auth",
		MethodName:   "internal_auth",
		AuthRequest:  req,
		AuthResponse: resp,
		Subject:      subject,
		Status:       getGRPCStatus(err),
	}
}

func ReportGRPCCall(ctx context.Context, req proto.Message, methodName string, containerID string, subject string, err error) {
	event := GRPCCallAuditEvent(ctx, methodName, req, containerID, subject, err)
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

	_, err = fmt.Fprintln(os.Stdout, string(jsonData))
	if err != nil {
		xlog.Error(ctx, "error reporting audit event", zap.Error(err))
	}
}
