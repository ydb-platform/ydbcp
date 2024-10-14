package backup_operations

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEndpointValidation(t *testing.T) {
	domains := []string{".valid.com", "hostname.good.com"}

	assert.True(t, IsAllowedEndpoint("grpc://some-host.zone.valid.com", domains, true))
	assert.False(t, IsAllowedEndpoint("grpcs://host.zone.invalid.com", domains, true))
	assert.True(t, IsAllowedEndpoint("grpcs://hostname.good.com:1234", domains, true))
	assert.True(t, IsAllowedEndpoint("example.valid.com:1234", domains, true))
	assert.False(t, IsAllowedEndpoint("grpcs://something.hostname.good.com:1234", domains, true))
	assert.False(t, IsAllowedEndpoint("", domains, true))
	assert.False(t, IsAllowedEndpoint("grpcs://evilvalid.com:1234", domains, true))
	assert.False(t, IsAllowedEndpoint("badhostname.good.com", domains, true))
	assert.False(t, IsAllowedEndpoint("some^bad$symbolvalid.com", domains, true))
}

func TestEndpointSecureValidation(t *testing.T) {
	domains := []string{".valid.com", "hostname.good.com"}

	assert.False(t, IsAllowedEndpoint("grpc://some-host.zone.valid.com", domains, false))
	assert.False(t, IsAllowedEndpoint("grpcs://host.zone.invalid.com", domains, false))
	assert.False(t, IsAllowedEndpoint("host.zone.valid.com", domains, false))
	assert.True(t, IsAllowedEndpoint("grpcs://hostname.good.com:1234", domains, false))
	assert.False(t, IsAllowedEndpoint("grpcs://something.hostname.good.com:1234", domains, false))
	assert.False(t, IsAllowedEndpoint("", domains, false))
	assert.False(t, IsAllowedEndpoint("grpcs://evilvalid.com:1234", domains, false))
	assert.False(t, IsAllowedEndpoint("badhostname.good.com", domains, false))
}

func TestSafePathJoin(t *testing.T) {
	p, ok := SafePathJoin("/test", "/path/to")
	assert.True(t, ok)
	assert.Equal(t, "/test/path/to", p)

	p, ok = SafePathJoin("/test", "")
	assert.True(t, ok)
	assert.Equal(t, "/test", p)

	_, ok = SafePathJoin("/test", "../to")
	assert.False(t, ok)

	p, ok = SafePathJoin("/test", "///to")
	assert.True(t, ok)
	assert.Equal(t, "/test/to", p)

	_, ok = SafePathJoin("/test/", "")
	assert.False(t, ok)
}
