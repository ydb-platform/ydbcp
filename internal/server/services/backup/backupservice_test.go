package backup

import (
	"testing"
	"ydbcp/internal/auth"
	"ydbcp/internal/config"
	"ydbcp/internal/connectors/client"
	"ydbcp/internal/connectors/db"

	"github.com/stretchr/testify/assert"
)

func TestEndpointValidation(t *testing.T) {
	dbConnector := db.NewMockDBConnector()
	clientConnector := client.NewMockClientConnector()
	auth := auth.NewMockAuthProvider()

	s := NewBackupService(
		dbConnector,
		clientConnector,
		config.S3Config{},
		auth,
		[]string{".valid.com", "hostname.good.com"},
		true,
	)

	assert.True(t, s.isAllowedEndpoint("grpc://some-host.zone.valid.com"))
	assert.False(t, s.isAllowedEndpoint("grpcs://host.zone.invalid.com"))
	assert.True(t, s.isAllowedEndpoint("grpcs://hostname.good.com:1234"))
	assert.True(t, s.isAllowedEndpoint("example.valid.com:1234"))
	assert.False(t, s.isAllowedEndpoint("grpcs://something.hostname.good.com:1234"))
	assert.False(t, s.isAllowedEndpoint(""))
	assert.False(t, s.isAllowedEndpoint("grpcs://evilvalid.com:1234"))
	assert.False(t, s.isAllowedEndpoint("badhostname.good.com"))
	assert.False(t, s.isAllowedEndpoint("some^bad$symbols.valid.com"))
}

func TestEndpointSecureValidation(t *testing.T) {
	dbConnector := db.NewMockDBConnector()
	clientConnector := client.NewMockClientConnector()
	auth := auth.NewMockAuthProvider()

	s := NewBackupService(
		dbConnector,
		clientConnector,
		config.S3Config{},
		auth,
		[]string{".valid.com", "hostname.good.com"},
		false,
	)

	assert.False(t, s.isAllowedEndpoint("grpc://some-host.zone.valid.com"))
	assert.False(t, s.isAllowedEndpoint("grpcs://host.zone.invalid.com"))
	assert.False(t, s.isAllowedEndpoint("host.zone.valid.com"))
	assert.True(t, s.isAllowedEndpoint("grpcs://hostname.good.com:1234"))
	assert.False(t, s.isAllowedEndpoint("grpcs://something.hostname.good.com:1234"))
	assert.False(t, s.isAllowedEndpoint(""))
	assert.False(t, s.isAllowedEndpoint("grpcs://evilvalid.com:1234"))
	assert.False(t, s.isAllowedEndpoint("badhostname.good.com"))
}
