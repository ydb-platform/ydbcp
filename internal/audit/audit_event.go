package audit

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"strings"
	"time"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/util/xlog"
)

var EventsDestination string

type Event struct { //flat event struct for everything
	ID             string
	IdempotencyKey string
	Action         Action
	Component      string
	MethodName     string
	ContainerID    string
	Subject        string
	SanitizedToken string
	Resource       Resource
	GRPCRequest    proto.Message
	Status         string
	Reason         string
	Timestamp      time.Time
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
	return json.Marshal(
		&struct {
			ID             string          `json:"request_id"`
			IdempotencyKey string          `json:"idempotency_key"`
			Service        string          `json:"service"`
			SpecVersion    string          `json:"specversion"`
			Action         string          `json:"action"`
			Resource       Resource        `json:"resource"`
			Component      string          `json:"component"`
			MethodName     string          `json:"operation,omitempty"`
			ContainerID    string          `json:"folder_id"`
			Subject        string          `json:"subject"`
			SanitizedToken string          `json:"sanitized_token"`
			GRPCRequest    json.RawMessage `json:"grpc_request,omitempty"`
			Status         string          `json:"status"`
			Reason         string          `json:"reason,omitempty"`
			Timestamp      string          `json:"timestamp"`
			IsBackground   bool            `json:"is_background"`
		}{
			ID:             e.ID,
			IdempotencyKey: e.IdempotencyKey,
			Service:        "ydbcp",
			SpecVersion:    "1.0",
			Action:         string(e.Action),
			Resource:       e.Resource,
			Component:      e.Component,
			MethodName:     e.MethodName,
			ContainerID:    e.ContainerID,
			Subject:        strings.Join([]string{e.Subject, "as"}, "@"),
			SanitizedToken: e.SanitizedToken,
			GRPCRequest:    marshalProtoMessage(e.GRPCRequest),
			Status:         e.Status,
			Reason:         e.Reason,
			Timestamp:      e.Timestamp.Format(time.RFC3339Nano),
		},
	)
}

func (ej *EventJson) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		&struct {
			Destination string `json:"destination,omitempty"`
			Event       *Event `json:"event"`
			Type        string `json:"type"`
		}{
			Destination: ej.Destination,
			Event:       ej.Event,
			Type:        ej.Type,
		},
	)
}

func getStatus(inProgress bool, err error) (string, string) {
	var status, reason string
	if err != nil {
		status = "ERROR"
		reason = err.Error()
	} else if inProgress {
		status = "IN-PROCESS"
	} else {
		status = "SUCCESS"
	}
	return status, reason
}

func GRPCCallAuditEvent(
	ctx context.Context,
	methodName string,
	req proto.Message,
	subject string,
	token string,
	containerID string,
	inProgress bool,
	err error,
) *Event {
	s, r := getStatus(inProgress, err)
	return &Event{
		ID:             uuid.New().String(),
		IdempotencyKey: grpcinfo.GetRequestID(ctx),
		Component:      "grpc_api",
		MethodName:     methodName,
		GRPCRequest:    req,
		ContainerID:    containerID,
		Subject:        subject,
		SanitizedToken: token,
		Action:         ActionFromMethodName(ctx, methodName),
		Resource:       ResourceFromMethodName(ctx, methodName),
		Status:         s,
		Reason:         r,
		Timestamp:      time.Now(),
	}
}

func ReportGRPCCallBegin(
	ctx context.Context, req proto.Message, methodName string,
	subject string, token string,
) {
	event := GRPCCallAuditEvent(
		ctx, methodName, req, subject, token, "{none}", true, nil,
	)
	ReportAuditEvent(ctx, event)
}

func ReportGRPCCallEnd(
	ctx context.Context, methodName string,
	subject string, containerID string, token string, err error,
) {
	event := GRPCCallAuditEvent(
		ctx, methodName, nil, subject, token, containerID, false, err,
	)
	ReportAuditEvent(ctx, event)
}

func ReportAuditEvent(ctx context.Context, event *Event) {
	jsonData, err := json.Marshal(
		&EventJson{
			Destination: EventsDestination,
			Event:       event,
			Type:        "ydbcp-audit",
		},
	)
	if err != nil {
		xlog.Error(ctx, "error reporting audit event", zap.Error(err))
		return
	}
	xlog.Raw(string(jsonData))
	if err != nil {
		xlog.Error(ctx, "error reporting audit event", zap.Error(err))
	}
}
