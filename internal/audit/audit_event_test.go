package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"io"
	"os"
	"testing"
	"time"
	"ydbcp/internal/server/grpcinfo"
	"ydbcp/internal/types"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

// Mock proto message

func TestMarshalProtoMessage(t *testing.T) {
	msg := &pb.GetBackupRequest{
		Id: "id1",
	}
	result := marshalProtoMessage(msg)
	assert.Contains(t, string(result), "id1")

	nilResult := marshalProtoMessage(nil)
	assert.Nil(t, nilResult)
}

func TestGRPCCallAuditEvent(t *testing.T) {
	ctx := context.Background()
	msg := &pb.GetBackupRequest{
		Id: "id1",
	}
	err := status.Error(codes.PermissionDenied, "denied")

	cid := "cid1"
	event := GRPCCallAuditEvent(
		ctx, pb.BackupScheduleService_GetBackupSchedule_FullMethodName,
		msg, "subj", "token.signature", cid,
		"", false, err,
	)

	assert.Equal(t, "grpc_api", event.Component)
	assert.Equal(t, "subj@as", event.Subject)
	assert.Equal(t, "cid1", event.FolderID)
	assert.Equal(t, pb.BackupScheduleService_GetBackupSchedule_FullMethodName, event.MethodName)
	assert.Equal(t, StatusError, event.Status)
	assert.Equal(t, marshalProtoMessage(msg), event.GRPCRequest)
}

func TestGRPCCallAuditEventWithNewFields(t *testing.T) {
	ctx := context.Background()
	traceID := "trace-123"
	ctx = context.WithValue(ctx, "trace_id", traceID)
	msg := &pb.GetBackupRequest{
		Id: "id1",
	}
	err := status.Error(codes.PermissionDenied, "denied")

	cid := "cid1"
	database := "test-database"
	event := GRPCCallAuditEvent(
		ctx, pb.BackupScheduleService_GetBackupSchedule_FullMethodName,
		msg, "subj", "token.signature", cid,
		database, false, err,
	)

	assert.Equal(t, "test-database", event.Database)
	assert.Equal(t, "trace-123", event.TraceID)
	assert.NotEmpty(t, event.RemoteAddress) // Ensure remote address is populated
}

func TestEventMarshalJSON(t *testing.T) {
	event := &GRPCCallEvent{
		GenericAuditFields: GenericAuditFields{
			Resource:       "resource",
			Action:         ActionGet,
			Component:      "grpc_api",
			Subject:        "sub@as",
			FolderID:       "id1",
			Database:       "mydb",
			SanitizedToken: "tok",
			Status:         "SUCCESS",
			Timestamp:      time.Now().Format(time.RFC3339Nano),
		},
		GRPCRequest: marshalProtoMessage(
			&pb.ListBackupsRequest{
				ContainerId: "id1",
			},
		),
		MethodName: "Method",
	}

	data, err := json.Marshal(event)
	assert.NoError(t, err)
	assert.Contains(t, string(data), `"resource":"resource"`)
	assert.Contains(t, string(data), `"component":"grpc_api"`)
	assert.Contains(t, string(data), `"subject":"sub@as`)
	assert.Contains(t, string(data), `"folder_id":"id1"`)
	assert.Contains(t, string(data), `"operation":"Method"`)
	assert.Contains(t, string(data), `"status":`)
	assert.Contains(t, string(data), `"@timestamp":`)
	assert.Contains(t, string(data), `"database":"mydb"`)
}

func TestEventMarshalJSONWithNewFields(t *testing.T) {
	event := &GRPCCallEvent{
		GenericAuditFields: GenericAuditFields{
			Resource:       "resource",
			Action:         ActionGet,
			Component:      "grpc_api",
			Subject:        "sub@as",
			FolderID:       "id1",
			Database:       "mydb",
			TraceID:        "trace-123",
			RemoteAddress:  "127.0.0.1",
			SanitizedToken: "tok",
			Status:         "SUCCESS",
			Timestamp:      time.Now().Format(time.RFC3339Nano),
		},
		GRPCRequest: marshalProtoMessage(
			&pb.ListBackupsRequest{
				ContainerId: "id1",
			},
		),
		MethodName: "Method",
	}

	data, err := json.Marshal(event)
	assert.NoError(t, err)
	assert.Contains(t, string(data), `"database":"mydb"`)
	assert.Contains(t, string(data), `"trace_id":"trace-123"`)
	assert.Contains(t, string(data), `"remote_address":"127.0.0.1"`)
}

func TestMakeEnvelope(t *testing.T) {
	event, err := makeEnvelope(&GRPCCallEvent{GenericAuditFields: GenericAuditFields{Component: "test"}})
	require.NoError(t, err)
	ej := &EventJson{
		Destination: "stdout",
		Event:       event,
	}

	data, err := json.Marshal(ej)
	assert.NoError(t, err)
	fmt.Print(string(data))
	assert.Contains(t, string(data), `"destination":"stdout"`)
	assert.Contains(t, string(data), `"text_data"`)
	assert.Contains(t, string(data), `"type":"ydbcp-audit"`)
	assert.Contains(t, string(data), `\"component\":\"test\"`)
}

func TestReportAuditEvent(t *testing.T) {
	ctx := context.Background()
	event := &GRPCCallEvent{
		MethodName: "reportTest",
		GenericAuditFields: GenericAuditFields{
			Status: "SUCCESS",
		},
	}

	oldStream := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	EventsDestination = "test-destination"
	ReportAuditEvent(ctx, event)

	err := w.Close()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	os.Stdout = oldStream

	output := buf.String()
	fmt.Print(output)
	assert.Contains(t, output, `"destination":"test-destination"`)
	assert.Contains(t, output, `"type":"ydbcp-audit"`)
	assert.Contains(t, output, `\"operation\":\"reportTest\"`)
	assert.Contains(t, output, "SUCCESS")
}

func CaptureEventAsString(t *testing.T, writeEvent func()) string {
	oldStream := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	writeEvent()

	err := w.Close()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	os.Stdout = oldStream
	return buf.String()
}

func TestBackupStateAudit(t *testing.T) {
	writeEvent := func(tbwr *types.TakeBackupWithRetryOperation, fromSchedule bool) func() {
		return func() {
			ctx := context.Background()
			ReportBackupStateAuditEvent(ctx, tbwr, fromSchedule)
		}
	}

	type TestCase struct {
		op           types.TakeBackupWithRetryOperation
		entries      []string
		fromSchedule bool
	}

	scID := "scheduleID"
	cases := []TestCase{
		{
			op: types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					ID:          "123",
					ContainerID: "cid",
					State:       types.OperationStateRunning,
				},
				ScheduleID: &scID,
				Retries:    1,
			},
			entries: []string{
				`\"status\":\"IN-PROCESS\"`,
				`\"schedule_id\":\"scheduleID\"`,
				`\"folder_id\":\"cid\"`,
				`\"component\":\"backup_service\"`,
				`\"reason\":\"New backup attempt started\"`,
				`\"attempt\":1`,
			},
			fromSchedule: false,
		},
		{
			op: types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					ID:          "123",
					ContainerID: "cid",
					State:       types.OperationStateRunning,
				},
				ScheduleID: &scID,
			},
			entries: []string{
				`\"status\":\"IN-PROCESS\"`,
				`\"schedule_id\":\"scheduleID\"`,
				`\"folder_id\":\"cid\"`,
				`\"component\":\"backup_schedule_service\"`,
				`\"reason\":\"New retryable backup attempt initiated\"`,
			},
			fromSchedule: true,
		},
		{
			op: types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					ID:          "123",
					ContainerID: "cid",
					State:       types.OperationStateRunning,
				},
				ScheduleID: &scID,
				Retries:    1,
			},
			entries: []string{
				`\"status\":\"IN-PROCESS\"`,
				`\"schedule_id\":\"scheduleID\"`,
				`\"folder_id\":\"cid\"`,
				`\"component\":\"backup_service\"`,
				`\"reason\":\"New backup attempt started\"`,
				`\"attempt\":1`,
			},
			fromSchedule: false,
		},
		{
			op: types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					ID:          "123",
					ContainerID: "cid",
					State:       types.OperationStateDone,
				},
				ScheduleID: &scID,
				Retries:    1,
			},
			entries: []string{
				`\"status\":\"SUCCESS\"`,
				`\"schedule_id\":\"scheduleID\"`,
				`\"folder_id\":\"cid\"`,
				`\"component\":\"backup_service\"`,
				`\"reason\":\"Backup complete\"`,
				`\"attempt\":1`,
			},
			fromSchedule: false,
		},
		{
			op: types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					ID:          "123",
					ContainerID: "cid",
					State:       types.OperationStateError,
				},
				ScheduleID: &scID,
				Retries:    1,
			},
			entries: []string{
				`\"status\":\"ERROR\"`,
				`\"schedule_id\":\"scheduleID\"`,
				`\"folder_id\":\"cid\"`,
				`\"component\":\"backup_service\"`,
				`\"reason\":\"Backup and all its retry attempts failed\"`,
				`\"attempt\":1`,
			},
			fromSchedule: false,
		},
		{
			op: types.TakeBackupWithRetryOperation{
				TakeBackupOperation: types.TakeBackupOperation{
					ID:          "123",
					ContainerID: "cid",
					State:       types.OperationStateCancelled,
				},
				ScheduleID: &scID,
				Retries:    1,
			},
			entries: []string{
				`\"status\":\"ERROR\"`,
				`\"schedule_id\":\"scheduleID\"`,
				`\"folder_id\":\"cid\"`,
				`\"component\":\"backup_service\"`,
				`\"reason\":\"Backup operation cancelled\"`,
				`\"attempt\":1`,
			},
			fromSchedule: false,
		},
	}

	for i, c := range cases {
		t.Run(
			fmt.Sprintf("%d", i), func(t *testing.T) {
				s := CaptureEventAsString(t, writeEvent(&c.op, c.fromSchedule))
				for _, entry := range c.entries {
					assert.Contains(t, s, entry)
				}
			},
		)
	}
}

func TestRemoteAddressFromCtx(t *testing.T) {
	ctx := context.Background()
	address := remoteAddressFromCtx(ctx)
	assert.Equal(t, "{none}", address)

	remoteAddr := "192.168.1.1"
	ctx = peer.NewContext(
		ctx, &peer.Peer{
			Addr: &mockAddr{address: remoteAddr},
		},
	)
	address = remoteAddressFromCtx(ctx)
	assert.Equal(t, remoteAddr, address)

	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x_forwarded_for", "10.0.0.1"))
	address = remoteAddressFromCtx(ctx)
	assert.Equal(t, "10.0.0.1,192.168.1.1", address)
}

// mockAddr is a mock implementation of net.Addr for testing purposes
type mockAddr struct {
	address string
}

func (m *mockAddr) Network() string {
	return "tcp"
}

func (m *mockAddr) String() string {
	return m.address
}

func TestWithGRPCInfo(t *testing.T) {
	ctx := context.Background()
	ctx = grpcinfo.WithGRPCInfo(ctx)
	SetAuditFieldsForRequest(
		ctx, &AuditFields{
			ContainerID: "container-1",
			Database:    "db-1",
		},
	)

	requestID, _ := grpcinfo.GetRequestID(ctx)
	fields := GetAuditFieldsForRequest(requestID)
	require.NotNil(t, fields)
	require.Equal(t, "container-1", fields.ContainerID)
	require.Equal(t, "db-1", fields.Database)
}

func TestReportGRPCCallBeginJSON(t *testing.T) {
	ctx := grpcinfo.WithGRPCInfo(context.Background())
	ctx = peer.NewContext(
		ctx, &peer.Peer{
			Addr: &mockAddr{address: "192.168.1.1"},
		},
	)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x_forwarded_for", "10.0.0.1"))

	req := &pb.GetBackupRequest{Id: "id-req"}
	method := pb.BackupScheduleService_GetBackupSchedule_FullMethodName
	subj := "subj"
	token := "tok.***"

	out := CaptureEventAsString(
		t, func() {
			ReportGRPCCallBegin(ctx, req, method, subj, token)
		},
	)

	var ej EventJson
	require.NoError(t, json.Unmarshal([]byte(out), &ej))

	var evt GRPCCallEvent
	require.NoError(t, json.Unmarshal([]byte(ej.Event.TextData), &evt))

	assert.Equal(t, method, evt.MethodName)
	assert.Equal(t, StatusInProcess, evt.Status)
	assert.Equal(t, formatSubject(subj), evt.Subject)
	assert.Equal(t, token, evt.SanitizedToken)
	assert.Equal(t, "{none}", evt.FolderID)
	assert.Equal(t, "{none}", evt.Database)
	requestID, _ := grpcinfo.GetRequestID(ctx)
	assert.Equal(t, requestID, evt.IdempotencyKey)
	assert.Contains(t, string(evt.GRPCRequest), "id-req")
	assert.Contains(t, evt.RemoteAddress, "10.0.0.1")
	assert.Contains(t, evt.RemoteAddress, "192.168.1.1")
}

func TestReportGRPCCallEndJSON(t *testing.T) {
	ctx := grpcinfo.WithGRPCInfo(context.Background())
	// add peer info and forwarded header
	ctx = peer.NewContext(
		ctx, &peer.Peer{
			Addr: &mockAddr{address: "192.168.2.2"},
		},
	)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x_forwarded_for", "172.16.0.5"))

	method := pb.BackupScheduleService_GetBackupSchedule_FullMethodName
	subj := "end-subj"
	token := "end-tok"
	database := "db-1"
	err := status.Error(codes.NotFound, "not found")

	out := CaptureEventAsString(
		t, func() {
			ReportGRPCCallEnd(ctx, method, subj, token, "", database, err)
		},
	)

	var ej EventJson
	require.NoError(t, json.Unmarshal([]byte(out), &ej))

	var evt GRPCCallEvent
	require.NoError(t, json.Unmarshal([]byte(ej.Event.TextData), &evt))

	assert.Equal(t, method, evt.MethodName)
	assert.Equal(t, formatSubject(subj), evt.Subject)
	assert.Equal(t, token, evt.SanitizedToken)
	assert.Equal(t, "{none}", evt.FolderID)
	assert.Equal(t, database, evt.Database)
	assert.Equal(t, StatusError, evt.Status)
	assert.Equal(t, err.Error(), evt.Reason)
	assert.Nil(t, evt.GRPCRequest)
	assert.Contains(t, evt.RemoteAddress, "172.16.0.5")
	assert.Contains(t, evt.RemoteAddress, "192.168.2.2")
}
