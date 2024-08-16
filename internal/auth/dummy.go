package auth

import (
	"context"
	"errors"
	"fmt"
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
	xlog.Info(ctx, "AuthProviderDummy init", zap.String("config", config))
	return nil
}

func (p *authProviderDummy) Finish(ctx context.Context) error {
	xlog.Info(ctx, "AuthProviderDummy finish")
	return nil
}

func (p *authProviderDummy) Authorize(
	ctx context.Context,
	token string,
	checks ...auth.AuthorizeCheck,
) (results []auth.AuthorizeResult, subject string, err error) {
	xlog.Info(
		ctx,
		"AuthProviderDummy Authorize",
		zap.String("token", auth.MaskToken(token)),
		zap.String("checks", fmt.Sprintf("%v", checks)),
	)
	if len(checks) == 0 {
		xlog.Error(ctx, "AuthProviderDummy AuthorizeCheck list is empty")
		return nil, "", errors.New("AuthorizeCheck list is empty")
	}

	results = make([]auth.AuthorizeResult, 0, len(checks))
	for range len(checks) {
		results = append(results, auth.AuthorizeResult{Code: auth.AuthCodeSuccess})
	}
	xlog.Info(ctx, "AuthProviderDummy Authorize result",
		zap.String("results", fmt.Sprintf("%v", results)),
		zap.String("subject", anonymousSubject),
	)
	return results, subject, nil
}

func NewDummyAuthProvider(ctx context.Context) (auth.AuthProvider, error) {
	p := &authProviderDummy{}
	if err := p.Init(ctx, ""); err != nil {
		return nil, err
	}
	return p, nil
}
