package grpcinfo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
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
