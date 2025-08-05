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
	"ydbcp/internal/auth"
	"ydbcp/internal/util/xlog"
	auth2 "ydbcp/pkg/plugins/auth"
)

var EventsDestination string

type Event struct { //flat event struct for everything
	Resource     string
	Action       Action
	Component    string
	MethodName   string
	Subject      string
	Token        string
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
		Resource     string          `json:"resource"`
		Action       Action          `json:"action"`
		Component    string          `json:"component"`
		MethodName   string          `json:"method_name"`
		Subject      string          `json:"subject"`
		Token        string          `json:"token"`
		GRPCRequest  json.RawMessage `json:"grpc_request"`
		AuthRequest  json.RawMessage `json:"auth_request"`
		AuthResponse json.RawMessage `json:"auth_response"`
		Status       json.RawMessage `json:"status"`
		Timestamp    string          `json:"timestamp"`
	}{
		Resource:     e.Resource,
		Action:       e.Action,
		Component:    e.Component,
		MethodName:   e.MethodName,
		Subject:      e.Subject,
		Token:        e.Token,
		GRPCRequest:  marshalProtoMessage(e.GRPCRequest),
		AuthRequest:  marshalProtoMessage(e.AuthRequest),
		AuthResponse: marshalProtoMessage(e.AuthResponse),
		Status:       marshalProtoMessage(e.Status.Proto()),
		Timestamp:    e.Timestamp.Format(time.RFC3339Nano),
	})
}

func (ej *EventJson) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Destination string `json:"destination"`
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
		return status.New(codes.OK, "")
	}
	return status.Convert(err)
}

func GRPCCallAuditEvent(ctx context.Context, methodName string, req proto.Message, token, subject string, err error) *Event {
	return &Event{
		Resource:    "testresource",
		Component:   "grpc_api",
		MethodName:  methodName,
		GRPCRequest: req,
		Subject:     subject,
		Token:       token,
		Action:      FromMethodName(ctx, methodName),
		Status:      getGRPCStatus(err),
	}
}

func AuthCallAuditEvent(req proto.Message, resp proto.Message, subject string, err error) *Event {
	return &Event{
		Component:    "iam_auth",
		AuthRequest:  req,
		AuthResponse: resp,
		Subject:      subject,
		Status:       getGRPCStatus(err),
	}
}

func ReportGRPCCall(ctx context.Context, req proto.Message, methodName string, subject string, err error) {
	token, err := auth.TokenFromGRPCContext(ctx)
	if err == nil {
		token = auth2.MaskToken(token)
	}
	event := GRPCCallAuditEvent(ctx, methodName, req, token, subject, err)
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
