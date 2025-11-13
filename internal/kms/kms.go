package kms

import (
	"context"

	"ydbcp/internal/config"
	"ydbcp/internal/plugin"
	"ydbcp/pkg/plugins/kms"
)

func NewKmsProvider(ctx context.Context, cfg config.PluginConfig) (kms.KmsProvider, error) {
	return plugin.Load[kms.KmsProvider](ctx, cfg, "KmsProvider", "kms")
}
