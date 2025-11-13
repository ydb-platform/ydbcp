package plugin

import (
	"context"
	"fmt"
	"plugin"

	"ydbcp/internal/config"
	"ydbcp/internal/util/xlog"

	"go.uber.org/zap"
)

type Plugin interface {
	Init(ctx context.Context, config string) error
}

// Load loads a Go plugin, looks up the provided symbol name and initializes it with the
// plugin configuration. The type parameter must be an interface that embeds the Init method.
func Load[T Plugin](
	ctx context.Context,
	cfg config.PluginConfig,
	symbolName string,
	pluginKind string,
) (T, error) {
	var zero T

	xlog.Info(ctx, fmt.Sprintf("Loading %s plugin", pluginKind), zap.String("plugin_path", cfg.PluginPath))

	plug, err := plugin.Open(cfg.PluginPath)
	if err != nil {
		return zero, fmt.Errorf("can't load %s plugin, path %s: %w", pluginKind, cfg.PluginPath, err)
	}

	symbol, err := plug.Lookup(symbolName)
	if err != nil {
		return zero, fmt.Errorf("can't lookup %s symbol, plugin path %s: %w", symbolName, cfg.PluginPath, err)
	}

	instance, ok := symbol.(T)
	if !ok {
		return zero, fmt.Errorf("can't cast %s symbol, plugin path %s", symbolName, cfg.PluginPath)
	}

	pluginConfig, err := cfg.ConfigurationString()
	if err != nil {
		return zero, fmt.Errorf("can't get %s plugin configuration: %w", pluginKind, err)
	}

	if err = instance.Init(ctx, pluginConfig); err != nil {
		return zero, fmt.Errorf("can't initialize %s plugin: %w", pluginKind, err)
	}

	return instance, nil
}
