package audit

import (
	"context"
	"encoding/json"
	"strings"
	"time"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/types"
	"ydbcp/internal/util/xlog"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var EventsDestination string

type GenericAuditFields struct {
	ID             string           `json:"request_id"`
	TraceID        string           `json:"trace_id,omitempty"`
	IdempotencyKey string           `json:"idempotency_key"`
	Service        string           `json:"service"`
	SpecVersion    string           `json:"specversion"`
	Action         Action           `json:"action,omitempty"`
	Resource       Resource         `json:"resource"`
	Component      string           `json:"component"`
	FolderID       string           `json:"folder_id"`
	Database       string           `json:"database"`
	Subject        string           `json:"subject"`
	SanitizedToken string           `json:"sanitized_token,omitempty"`
	RemoteAddress  string           `json:"remote_address,omitempty"`
	Status         AuditEventStatus `json:"status"`
	Reason         string           `json:"reason,omitempty"`
	Timestamp      string           `json:"@timestamp"`
	IsBackground   bool             `json:"is_background"`
}

type HasEnrichLogContext interface {
	EnrichLogContext(ctx context.Context) context.Context
}

func (g *GenericAuditFields) EnrichLogContext(ctx context.Context) context.Context {
	ctx = xlog.With(ctx, zap.String("database", g.Database))
	ctx = xlog.With(ctx, zap.String("containerID", g.FolderID))
	ctx = xlog.With(ctx, zap.String("requestID", g.ID))
	ctx = xlog.With(ctx, zap.String("traceID", g.TraceID))
	return ctx
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
	GRPCRequest json.RawMessage `json:"grpc_request,omitempty"`
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

func formatContainerID(containerID string) string {
	switch containerID {
	case "", "{none}":
		return "{none}"
	default:
		return containerID
	}
}

func formatDatabase(database string) string {
	switch database {
	case "", "{none}":
		return "{none}"
	default:
		return database
	}
}

func formatSubject(subject string) string {
	switch subject {
	case "", "{none}":
		return "{none}"
	default:
		return subject + "@as"
	}
}

func formatTraceID(traceID *string) string {
	if traceID == nil {
		return "{none}"
	}
	return *traceID
}

func remoteAddressFromCtx(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		xlog.Error(ctx, "could not get peer info")
		return "{none}"
	} else {
		lastAddress := p.Addr.String()
		pref := grpcinfo.GetRemoteAddressChain(ctx)
		if pref != nil {
			return strings.Join([]string{*pref, lastAddress}, ",")
		}
		return lastAddress
	}
}

func GRPCCallAuditEvent(
	ctx context.Context,
	methodName string,
	req proto.Message,
	subject string,
	token string,
	containerID string,
	database string,
	inProgress bool,
	err error,
) *GRPCCallEvent {
	s, r := getStatus(inProgress, err)
	requestID, _ := grpcinfo.GetRequestID(ctx)
	return &GRPCCallEvent{
		GenericAuditFields: GenericAuditFields{
			ID:             uuid.New().String(),
			IdempotencyKey: requestID,
			TraceID:        formatTraceID(grpcinfo.GetTraceID(ctx)),
			Service:        "ydbcp",
			SpecVersion:    "1.0",
			Action:         ActionFromMethodName(ctx, methodName),
			Resource:       ResourceFromMethodName(ctx, methodName),
			Component:      "grpc_api",
			FolderID:       formatContainerID(containerID),
			Database:       formatDatabase(database),
			Subject:        formatSubject(subject),
			SanitizedToken: token,
			RemoteAddress:  remoteAddressFromCtx(ctx),
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
		ctx, methodName, req, subject, token, "{none}", "{none}", true, nil,
	)
	ReportAuditEvent(ctx, event)
}

func ReportGRPCCallEnd(
	ctx context.Context, methodName string,
	subject string, token string, containerID string, database string, err error,
) {
	event := GRPCCallAuditEvent(
		ctx, methodName, nil, subject, token, containerID, database, false, err,
	)
	ReportAuditEvent(ctx, event)
}

type BackupStateEvent struct {
	GenericAuditFields
	ScheduleID string `json:"schedule_id,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
}

func (b *BackupStateEvent) EnrichLogContext(ctx context.Context) context.Context {
	ctx = b.GenericAuditFields.EnrichLogContext(ctx)
	ctx = xlog.With(ctx, zap.String("ScheduleID", b.ScheduleID))
	return ctx
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
			Database:  operation.GetDatabaseName(),
			Subject:   types.OperationCreatorName,
			//no token
			Status:       status,
			Reason:       reason,
			Timestamp:    time.Now().Format(time.RFC3339Nano),
			IsBackground: true,
		},
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
	ScheduleID string `json:"schedule_id"`
}

func (b *FailedRPOAuditEvent) EnrichLogContext(ctx context.Context) context.Context {
	ctx = b.GenericAuditFields.EnrichLogContext(ctx)
	ctx = xlog.With(ctx, zap.String("ScheduleID", b.ScheduleID))
	return ctx
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
			Database:  schedule.DatabaseName,
			Subject:   types.OperationCreatorName,
			//no token
			Status:       StatusError,
			Reason:       "Recovery point objective failed for schedule",
			Timestamp:    time.Now().Format(time.RFC3339Nano),
			IsBackground: true,
		},
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
	if a, ok := event.(HasEnrichLogContext); ok {
		xlog.Debug(a.EnrichLogContext(ctx), env.TextData)
	}
}
