package auth

import (
	"context"
	"testing"

	"ydbcp/pkg/plugins/auth"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestAuthErrorError tests the AuthError.Error() method
func TestAuthErrorError(t *testing.T) {
	authErr := &AuthError{
		Code:        auth.AuthCodePermissionDenied,
		Message:     "access denied",
		Subject:     "service-account-123",
		Permission:  "ydb.databases.backup",
		ContainerID: "container-456",
	}

	expected := "authorization failed: subject=service-account-123, permission=ydb.databases.backup, container=container-456, code=PERMISSION_DENIED, message=access denied"
	assert.Equal(t, expected, authErr.Error())
}

// TestAuthErrorErrorWithEmptyFields tests AuthError formatting with empty fields
func TestAuthErrorErrorWithEmptyFields(t *testing.T) {
	authErr := &AuthError{
		Code:        auth.AuthCodeInvalidToken,
		Message:     "invalid token format",
		Subject:     "",
		Permission:  "ydb.databases.get",
		ContainerID: "",
	}

	errMsg := authErr.Error()
	assert.Contains(t, errMsg, "authorization failed")
	assert.Contains(t, errMsg, "INVALID_TOKEN")
	assert.Contains(t, errMsg, "invalid token format")
}

// TestFormatAuthError tests the formatAuthError function
func TestFormatAuthError(t *testing.T) {
	code := auth.AuthCodePermissionDenied
	message := "user lacks required role"
	subject := "user-id-789"
	permission := "ydb.databases.restore"
	containerID := "db-container"

	authErr := formatAuthError(code, message, subject, permission, containerID)

	assert.NotNil(t, authErr)
	assert.Equal(t, code, authErr.Code)
	assert.Equal(t, message, authErr.Message)
	assert.Equal(t, subject, authErr.Subject)
	assert.Equal(t, permission, authErr.Permission)
	assert.Equal(t, containerID, authErr.ContainerID)
	assert.Nil(t, authErr.InternalError)
}

// TestCheckAuthSuccess tests successful authorization
func TestCheckAuthSuccess(t *testing.T) {
	// Build incoming context with metadata
	md := metadata.New(
		map[string]string{
			"authorization": "Bearer valid-token",
		},
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	container := NewMocContainer()
	container.AddSubjectPermission("test-subject", PermissionBackupCreate)
	mockProvider := NewMockAuthProvider(
		WithToken("valid-token", "test-subject", auth.AuthCodeSuccess),
		WithContainer("container-id", container),
	)

	subject, err := CheckAuth(ctx, mockProvider, PermissionBackupCreate, "container-id", "")

	assert.NoError(t, err)
	assert.Equal(t, "test-subject", subject)
}

// TestCheckAuthPermissionDenied tests authorization failure due to permission denied
func TestCheckAuthPermissionDenied(t *testing.T) {
	md := metadata.New(
		map[string]string{
			"authorization": "Bearer denied-token",
		},
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	mockProvider := NewMockAuthProvider(
		WithToken("denied-token", "limited-user", auth.AuthCodePermissionDenied),
	)

	subject, err := CheckAuth(ctx, mockProvider, PermissionBackupCreate, "container-id", "")

	assert.Error(t, err)
	assert.Empty(t, subject)

	// Verify error is PermissionDenied gRPC status
	statusErr, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.PermissionDenied, statusErr.Code())

	// Verify error message contains readable information
	errMsg := statusErr.Message()
	assert.Contains(t, errMsg, "limited-user")
	assert.Contains(t, errMsg, PermissionBackupCreate)
	assert.Contains(t, errMsg, "container-id")
	assert.Contains(t, errMsg, "PERMISSION_DENIED")
}

// TestCheckAuthInvalidToken tests authorization failure due to invalid token
func TestCheckAuthInvalidToken(t *testing.T) {
	md := metadata.New(
		map[string]string{
			"authorization": "Bearer invalid-token",
		},
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	mockProvider := NewMockAuthProvider()

	subject, err := CheckAuth(ctx, mockProvider, PermissionBackupGet, "container-123", "")

	assert.Error(t, err)
	assert.Empty(t, subject)

	statusErr, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.PermissionDenied, statusErr.Code())
}

// TestCheckAuthUnknownSubject tests authorization failure due to unknown subject
func TestCheckAuthUnknownSubject(t *testing.T) {
	md := metadata.New(
		map[string]string{
			"authorization": "Bearer unknown-subject-token",
		},
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	mockProvider := NewMockAuthProvider(
		WithToken("unknown-subject-token", "abcde", auth.AuthCodeUnknownSubject),
	)

	subject, err := CheckAuth(ctx, mockProvider, PermissionBackupList, "container-xyz", "")

	assert.Error(t, err)
	assert.Empty(t, subject)

	statusErr, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.PermissionDenied, statusErr.Code())
	assert.Contains(t, err.Error(), "subject=abcde")
	assert.Contains(t, err.Error(), "permission=ydb.databases.list")
	assert.Contains(t, err.Error(), "container=container-xyz")
}
