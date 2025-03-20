package config

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigDefaults(t *testing.T) {
	ctx := context.Background()
	f, err := os.CreateTemp("", "config")
	assert.Empty(t, err)
	fileName := f.Name()
	defer os.Remove(fileName)
	assert.Empty(t, f.Close())
	cfg, err := InitConfig[Config](ctx, fileName)
	assert.Empty(t, err)
	assert.Equal(t, uint16(8080), cfg.MetricsServer.BindPort)
}
