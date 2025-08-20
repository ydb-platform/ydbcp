package main

import (
	"context"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"testing"
	"ydbcp/internal/auth"
)

func TestMaskedToken(t *testing.T) {
	provider := authProviderNebius{
		endpoint: "none",
		tls:      true,
		config: pluginConfig{
			AccessServiceEndpoint: "none",
			Insecure:              false,
			RootCAPath:            "path",
		},
	}
	ctx := metadata.NewIncomingContext(
		context.Background(), map[string][]string{
			"authorization": {"Bearer token.signature"},
		},
	)
	token, err := auth.GetMaskedToken(ctx, &provider)
	require.NoError(t, err)
	require.Equal(t, "token", token)
}
