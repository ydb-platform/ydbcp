package grpcinfo

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"testing"
	"ydbcp/internal/util/xlog"
)

func TestGetRemoteAddressChain(t *testing.T) {
	ctx := context.Background()

	assert.Nil(t, GetRemoteAddressChain(ctx))

	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x_forwarded_for", "192.168.1.1"))
	assert.Equal(t, "192.168.1.1", *GetRemoteAddressChain(ctx))

	ctx = metadata.NewIncomingContext(
		ctx, metadata.Pairs(
			"x_forwarded_for", "192.168.1.1",
		),
	)
	assert.Equal(t, "192.168.1.1", *GetRemoteAddressChain(ctx))
}

func TestGetGRPCHeaderValue(t *testing.T) {
	ctx := context.Background()

	assert.Nil(t, GetGRPCHeaderValue(ctx, "key"))

	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("key", "value"))
	assert.Equal(t, "value", *GetGRPCHeaderValue(ctx, "key"))

	assert.Nil(t, GetGRPCHeaderValue(ctx, "nonexistent_key"))
}

func TestGetTraceID(t *testing.T) {
	ctx := context.Background()

	assert.Nil(t, GetTraceID(ctx))

	traceID := "trace-123"
	ctx = context.WithValue(ctx, "trace_id", traceID)
	assert.Equal(t, traceID, *GetTraceID(ctx))
}

func TestRequestID(t *testing.T) {
	ctx := context.Background()

	id, generated := GetRequestID(ctx)
	require.True(t, generated)

	ctx = SetRequestID(ctx, id)

	id2, generated := GetRequestID(ctx)
	require.False(t, generated)
	require.Equal(t, id, id2)
}

func TestWithGRPCInfo(t *testing.T) {
	logger, err := xlog.SetupLogging("DEBUG")
	require.NoError(t, err)
	xlog.SetInternalLogger(logger)

	ctx := context.Background()
	ctx = WithGRPCInfo(ctx)

	id, generated := GetRequestID(ctx)
	require.False(t, generated)
	id2, generated := GetRequestID(ctx)
	require.False(t, generated)
	require.Equal(t, id, id2)
}
