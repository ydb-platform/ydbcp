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
		ctx, pb.BackupScheduleService_GetBackupSchedule_FullMethodName, msg, "subj", "token", cid, false, err,
	)

	assert.Equal(t, "grpc_api", event.Component)
	assert.Equal(t, "subj", event.Subject)
	assert.Equal(t, "cid1", event.ContainerID)
	assert.Equal(t, pb.BackupScheduleService_GetBackupSchedule_FullMethodName, event.MethodName)
	assert.Equal(t, "ERROR", event.Status)
	assert.Equal(t, msg, event.GRPCRequest)
}

func TestEventMarshalJSON(t *testing.T) {
	event := &Event{
		Resource:       "resource",
		Action:         ActionGet,
		Component:      "grpc_api",
		MethodName:     "Method",
		Subject:        "sub",
		SanitizedToken: "tok",
		GRPCRequest: &pb.ListBackupsRequest{
			ContainerId: "id1",
		},
		Status:    "SUCCESS",
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(event)
	assert.NoError(t, err)
	assert.Contains(t, string(data), `"resource":"resource"`)
	assert.Contains(t, string(data), `"component":"grpc_api"`)
	assert.Contains(t, string(data), `"subject":"sub@as`)
	assert.Contains(t, string(data), `"container_id":"id1"`)
	assert.Contains(t, string(data), `"status":`)
	assert.Contains(t, string(data), `"@timestamp":`)
}

func TestEventJsonMarshal(t *testing.T) {
	event := &Event{Component: "test"}
	ej := &EventJson{
		Destination: "stdout",
		Event:       event,
		Type:        "ydbcp-audit",
	}

	data, err := json.Marshal(ej)
	assert.NoError(t, err)
	fmt.Print(string(data))
	assert.Contains(t, string(data), `"destination":"stdout"`)
	assert.Contains(t, string(data), `"type":"ydbcp-audit"`)
	assert.Contains(t, string(data), `"component":"test"`)
}

func TestReportAuditEvent(t *testing.T) {
	ctx := context.Background()
	event := &Event{
		MethodName: "reportTest",
		Status:     "SUCCESS",
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
	assert.Contains(t, output, `"operation":"reportTest"`)
	assert.Contains(t, output, "SUCCESS")
}
