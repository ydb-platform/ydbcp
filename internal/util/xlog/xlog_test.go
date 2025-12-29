package xlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWithRetainsFields(t *testing.T) {
	observed := SetupLoggingWithObserver()

	ctx := context.Background()
	ctx = With(ctx, zap.String("db", "mydb"))
	ctx = With(ctx, zap.String("traceID", "t1"))

	Info(ctx, "test-retain")

	require.Equal(t, 1, observed.Len(), "expected exactly one log entry")
	require.True(
		t, observed.FilterField(zap.String("db", "mydb")).Len() == 1,
	)
	require.True(
		t, observed.FilterField(zap.String("traceID", "t1")).Len() == 1,
	)
}
