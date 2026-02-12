package auth

import (
	"context"
	"errors"
	"fmt"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/auth"

	"go.uber.org/zap"
)

type authProviderDummy struct {
}

const (
	anonymousSubject = "anonymous"
)

func (p *authProviderDummy) Init(ctx context.Context, config string) error {
	xlog.Info(ctx, "AuthProviderDummy init", zap.String(log_keys.Config, config))
	return nil
}

func (p *authProviderDummy) Finish(ctx context.Context) error {
	xlog.Info(ctx, "AuthProviderDummy finish")
	return nil
}

func (p *authProviderDummy) Authorize(
	ctx context.Context,
	_ string,
	checks ...auth.AuthorizeCheck,
) (results []auth.AuthorizeResult, subject string, err error) {
	xlog.Info(
		ctx,
		"AuthProviderDummy Authorize",
		zap.String(log_keys.AuthorizeChecks, fmt.Sprintf("%v", checks)),
	)
	if len(checks) == 0 {
		xlog.Error(ctx, "AuthProviderDummy AuthorizeCheck list is empty")
		return nil, "", errors.New("AuthorizeCheck list is empty")
	}

	results = make([]auth.AuthorizeResult, 0, len(checks))
	for range len(checks) {
		results = append(results, auth.AuthorizeResult{Code: auth.AuthCodeSuccess})
	}
	xlog.Info(
		ctx, "AuthProviderDummy Authorize result",
		zap.String(log_keys.AuthResults, fmt.Sprintf("%v", results)),
		zap.String(log_keys.Subject, anonymousSubject),
	)
	return results, subject, nil
}

func (p *authProviderDummy) Authenticate(
	ctx context.Context,
	_ string,
) (string, error) {
	xlog.Info(
		ctx,
		"AuthProviderDummy Authenticate",
	)
	xlog.Info(
		ctx, "AuthProviderDummy Authorize result",
		zap.String(log_keys.Subject, anonymousSubject),
	)
	return anonymousSubject, nil
}

func (p *authProviderDummy) MaskToken(token string) string {
	return token
}

func (p *authProviderDummy) GetYDBCPContainerID() string {
	return ""
}

func NewDummyAuthProvider(ctx context.Context) (auth.AuthProvider, error) {
	p := &authProviderDummy{}
	if err := p.Init(ctx, ""); err != nil {
		return nil, err
	}
	return p, nil
}
