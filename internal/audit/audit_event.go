package audit

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"time"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"
)

var EventsDestination string

type GenericAuditFields struct {
	ID             string           `json:"request_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	Service        string           `json:"service"`
	SpecVersion    string           `json:"specversion"`
	Action         Action           `json:"action,omitempty"`
	Resource       Resource         `json:"resource"`
	Component      string           `json:"component"`
	FolderID       string           `json:"folder_id"`
	Subject        string           `json:"subject"`
	SanitizedToken string           `json:"sanitized_token,omitempty"`
	Status         AuditEventStatus `json:"status"`
	Reason         string           `json:"reason,omitempty"`
	Timestamp      string           `json:"@timestamp"`
	IsBackground   bool             `json:"is_background"`
}

type EventEnvelope struct {
	TextData string `json:"text_data"`
	Type     string `json:"type"`
}

type EventJson struct {
	Destination string         `json:"destination,omitempty"`
	Event       *EventEnvelope `json:"event"`
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

type GRPCCallEvent struct {
	GenericAuditFields

	MethodName  string          `json:"operation"`
	GRPCRequest json.RawMessage `json:"grpc_request"`
}

func makeEnvelope(event any) (*EventEnvelope, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return &EventEnvelope{
		TextData: string(data),
		Type:     "ydbcp-audit",
	}, nil
}

type AuditEventStatus string

var (
	StatusInProcess AuditEventStatus = "IN-PROCESS"
	StatusError     AuditEventStatus = "ERROR"
	StatusSuccess   AuditEventStatus = "SUCCESS"
)

func getStatus(inProgress bool, err error) (AuditEventStatus, string) {
	var status AuditEventStatus
	var reason string
	if err != nil {
		status = StatusError
		reason = err.Error()
	} else if inProgress {
		status = StatusInProcess
	} else {
		status = StatusSuccess
	}
	return status, reason
}

func formatSubject(subject string) string {
	switch subject {
	case "", "{none}":
		return "{none}"
	default:
		return subject + "@as"
	}
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
) *GRPCCallEvent {
	s, r := getStatus(inProgress, err)
	return &GRPCCallEvent{
		GenericAuditFields: GenericAuditFields{
			ID:             uuid.New().String(),
			IdempotencyKey: grpcinfo.GetRequestID(ctx),
			Service:        "ydbcp",
			SpecVersion:    "1.0",
			Action:         ActionFromMethodName(ctx, methodName),
			Resource:       ResourceFromMethodName(ctx, methodName),
			Component:      "grpc_api",
			FolderID:       containerID,
			Subject:        formatSubject(subject),
			SanitizedToken: token,
			Status:         s,
			Reason:         r,
			Timestamp:      time.Now().Format(time.RFC3339Nano),
			IsBackground:   false,
		},
		MethodName:  methodName,
		GRPCRequest: marshalProtoMessage(req),
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

type BackupStateEvent struct {
	GenericAuditFields
	Database   string `json:"database"`
	ScheduleID string `json:"schedule_id,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
}

func ReportBackupStateAuditEvent(
	ctx context.Context, operation *types.TakeBackupWithRetryOperation, fromScheduleHandler bool,
) {
	var status AuditEventStatus
	reason := ""
	component := "backup_service"
	switch operation.GetState() {
	case types.OperationStateRunning:
		{
			status = StatusInProcess
			if !fromScheduleHandler {
				reason = "New backup attempt started"
			} else {
				component = "backup_schedule_service"
				reason = "New retryable backup attempt initiated"
			}
		}
	case types.OperationStateDone:
		{
			status = StatusSuccess
			reason = "Backup complete"
		}
	case types.OperationStateError:
		{
			status = StatusError
			reason = "Backup and all its retry attempts failed"
		}
	case types.OperationStateCancelling,
		types.OperationStateCancelled,
		types.OperationStateStartCancelling:
		{
			status = StatusError
			reason = "Backup operation cancelled"
		}
	}

	event := &BackupStateEvent{
		GenericAuditFields: GenericAuditFields{
			ID:             uuid.New().String(),
			IdempotencyKey: operation.GetID(),
			Service:        "ydbcp",
			SpecVersion:    "1.0",
			//no action
			Resource:  Backup,
			Component: component,
			FolderID:  operation.GetContainerID(),
			Subject:   types.OperationCreatorName,
			//no token
			Status:       status,
			Reason:       reason,
			Timestamp:    time.Now().Format(time.RFC3339Nano),
			IsBackground: true,
		},
		Database: operation.GetDatabaseName(),
	}
	if operation.ScheduleID != nil {
		event.ScheduleID = *operation.ScheduleID
	}
	if !fromScheduleHandler {
		event.Attempt = operation.Retries
	}

	ReportAuditEvent(ctx, event)
}

type FailedRPOAuditEvent struct {
	GenericAuditFields
	Database   string `json:"database"`
	ScheduleID string `json:"schedule_id"`
}

var ReportedMissedRPOs = make(map[string]bool)

func ReportFailedRPOAuditEvent(ctx context.Context, schedule *types.BackupSchedule) {
	if schedule == nil {
		xlog.Error(ctx, "nil schedule passed to ReportFailedRPOAuditEvent")
		return
	}
	if ReportedMissedRPOs[schedule.ID] {
		return
	}
	event := &FailedRPOAuditEvent{
		GenericAuditFields: GenericAuditFields{
			ID:             uuid.New().String(),
			IdempotencyKey: schedule.ID,
			Service:        "ydbcp",
			SpecVersion:    "1.0",
			//no action
			Resource:  BackupSchedule,
			Component: "backup_schedule_service",
			FolderID:  schedule.ContainerID,
			Subject:   types.OperationCreatorName,
			//no token
			Status:       StatusError,
			Reason:       "Recovery point objective failed for schedule",
			Timestamp:    time.Now().Format(time.RFC3339Nano),
			IsBackground: true,
		},
		Database:   schedule.DatabaseName,
		ScheduleID: schedule.ID,
	}
	ReportAuditEvent(ctx, event)
	ReportedMissedRPOs[schedule.ID] = true
}

func ReportAuditEvent(ctx context.Context, event any) {
	env, err := makeEnvelope(event)
	if err != nil {
		xlog.Error(ctx, "error reporting audit event", zap.Error(err))
		return
	}
	jsonData, err := json.Marshal(
		&EventJson{
			Destination: EventsDestination,
			Event:       env,
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
