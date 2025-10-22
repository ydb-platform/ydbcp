package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"os"
	"testing"
	"time"
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
