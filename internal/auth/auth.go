package auth

import (
	"context"
	"fmt"
	"plugin"

	"ydbcp/internal/config"
	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/auth"

	"go.uber.org/zap"
)

const (
	PermissionBackupList    = "ydb.databases.list"
	PermissionBackupCreate  = "ydb.databases.backup"
	PermissionBackupRestore = "ydb.tables.create"
	PermissionBackupGet     = "ydb.databases.get"
)

func NewAuthProvider(ctx context.Context, cfg config.AuthConfig) (auth.AuthProvider, error) {
	xlog.Info(ctx, "Loading auth provider plugin", zap.String("path", cfg.PluginPath))

	plug, err := plugin.Open(cfg.PluginPath)
	if err != nil {
		return nil, fmt.Errorf("can't load auth provider plugin, path %s: %w", cfg.PluginPath, err)
	}
	symbol, err := plug.Lookup("AuthProvider")
	if err != nil {
		return nil, fmt.Errorf("can't lookup AuthProvider symbol, plugin path %s: %w", cfg.PluginPath, err)
	}
	var instance auth.AuthProvider
	instance, ok := symbol.(auth.AuthProvider)
	if !ok {
		return nil, fmt.Errorf("can't cast AuthProvider symbol, plugin path %s", cfg.PluginPath)
	}
	pluginConfig, err := cfg.ConfigurationString()
	if err != nil {
		return nil, fmt.Errorf("can't get auth provider configuration: %w", err)
	}
	if err = instance.Init(ctx, pluginConfig); err != nil {
		return nil, fmt.Errorf("can't initialize auth provider plugin: %w", err)
	}
	return instance, nil
}
