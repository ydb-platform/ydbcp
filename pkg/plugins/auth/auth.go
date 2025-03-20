package auth

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"strings"
)

type AuthCode uint

const (
	AuthCodeSuccess          = AuthCode(0)
	AuthCodeError            = AuthCode(1)
	AuthCodeInvalidToken     = AuthCode(2)
	AuthCodeUnknownSubject   = AuthCode(3)
	AuthCodePermissionDenied = AuthCode(4)
)

type AuthorizeCheck struct {
	Permission  string
	ContainerID string
	ResourceID  []string
}

type AuthorizeResult struct {
	Code    AuthCode
	Message string
}

type AuthProvider interface {
	Init(ctx context.Context, config string) error
	Finish(ctx context.Context) error
	GetTLSOption() (grpc.DialOption, error)
	Authorize(ctx context.Context, token string, checks ...AuthorizeCheck) (results []AuthorizeResult, subjectID string, err error)
}

func (code AuthCode) String() string {
	switch code {
	case AuthCodeSuccess:
		return "SUCCESS"
	case AuthCodeError:
		return "ERROR"
	case AuthCodeInvalidToken:
		return "INVALID_TOKEN"
	case AuthCodePermissionDenied:
		return "PERMISSION_DENIED"
	}
	return "UNKNOWN"
}

func (r *AuthorizeResult) String() string {
	return fmt.Sprintf("AuthorizeResult{code: %s, message: %s}", r.Code.String(), r.Message)
}

func (c *AuthorizeCheck) String() string {
	return fmt.Sprintf(
		"AuthorizeCheck{permission: %s, containerID: %s, resourceIDs: %s}",
		c.Permission, c.ContainerID, strings.Join(c.ResourceID, ", "),
	)
}

func MaskToken(token string) string {
	r := []rune(token)
	if len(r) < 6 {
		return "***"
	}
	return fmt.Sprintf("%s...%s", string(r[0:3]), string(r[len(r)-3:]))
}
