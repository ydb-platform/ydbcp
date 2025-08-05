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

	event := GRPCCallAuditEvent(ctx, "TestService/TestMethod", msg, "user1", err)

	assert.Equal(t, "grpc_api", event.Component)
	assert.Equal(t, "user1", event.Subject)
	assert.Equal(t, "some-token", event.Token)
	assert.Equal(t, "TestService/TestMethod", event.MethodName)
	assert.Equal(t, codes.PermissionDenied, event.Status.Code())
	assert.Equal(t, msg, event.GRPCRequest)
}

func TestAuthCallAuditEvent(t *testing.T) {
	req := &pb.GetBackupRequest{
		Id: "id1",
	}
	resp := &pb.Backup{Id: "id1"}
	err := status.Error(codes.Unauthenticated, "bad creds")

	event := AuthCallAuditEvent(req, resp, "userX", err)

	assert.Equal(t, "iam_auth", event.Component)
	assert.Equal(t, "userX", event.Subject)
	assert.Equal(t, codes.Unauthenticated, event.Status.Code())
	assert.Equal(t, req, event.AuthRequest)
	assert.Equal(t, resp, event.AuthResponse)
}

func TestEventMarshalJSON(t *testing.T) {
	event := &Event{
		Resource:   "resource",
		Action:     ActionGet,
		Component:  "grpc_api",
		MethodName: "Method",
		Subject:    "sub",
		Token:      "tok",
		GRPCRequest: &pb.GetBackupRequest{
			Id: "id1",
		},
		Status:    status.New(codes.OK, "ok"),
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(event)
	assert.NoError(t, err)
	assert.Contains(t, string(data), `"resource":"resource"`)
	assert.Contains(t, string(data), `"component":"grpc_api"`)
	assert.Contains(t, string(data), `"status":`)
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

func TestGetGRPCStatus(t *testing.T) {
	err := status.Error(codes.InvalidArgument, "bad input")
	s := getGRPCStatus(err)
	assert.Equal(t, codes.InvalidArgument, s.Code())

	s = getGRPCStatus(nil)
	assert.Equal(t, codes.OK, s.Code())
}

func TestReportAuditEvent(t *testing.T) {
	ctx := context.Background()
	event := &Event{
		MethodName: "reportTest",
		Status:     status.New(codes.OK, ""),
		Timestamp:  time.Now(),
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
	assert.Contains(t, output, `"destination":"test-destination"`)
	assert.Contains(t, output, `"type":"ydbcp-audit"`)
	assert.Contains(t, output, `"method_name":"reportTest"`)
}
