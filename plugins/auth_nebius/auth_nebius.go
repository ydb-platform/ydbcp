package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"ydbcp/internal/util/log_keys"
	"ydbcp/internal/util/tls_setup"

	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/auth"
	pb "ydbcp/plugins/auth_nebius/proto/iam/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

type authProviderNebius struct {
	endpoint         string
	tls              bool
	config           pluginConfig
	ydbcpContainerID string
}

type pluginConfig struct {
	AccessServiceEndpoint string `yaml:"access_service_endpoint"`
	Insecure              bool   `yaml:"insecure" default:"false"`
	RootCAPath            string `yaml:"root_ca_path"`
}

func (p *authProviderNebius) loadTLSCredentials() (grpc.DialOption, error) {
	if !p.tls {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	return tls_setup.LoadTLSCredentials(&p.config.RootCAPath, p.config.Insecure)
}

func (p *authProviderNebius) Init(ctx context.Context, config string) error {
	xlog.Info(ctx, "AuthProviderNebius init", zap.String(log_keys.Config, config))
	err := yaml.Unmarshal([]byte(config), &p.config)
	if err != nil {
		xlog.Error(ctx, "Unable to parse configuration file", zap.Error(err))
		return fmt.Errorf("unable to parse configuration %w", err)
	}
	p.endpoint, p.tls = tls_setup.ParseEndpoint(p.config.AccessServiceEndpoint)
	return nil
}

func (p *authProviderNebius) Finish(ctx context.Context) error {
	xlog.Info(ctx, "AuthProviderNebius finish")
	return nil
}

func (p *authProviderNebius) client() (*grpc.ClientConn, error) {
	tlsOption, err := p.loadTLSCredentials()
	if err != nil {
		return nil, err
	}
	return grpc.NewClient("dns:"+p.endpoint, tlsOption)
}

func authenticateRequest(token string) *pb.AuthenticateRequest {
	return &pb.AuthenticateRequest{
		Credentials: &pb.AuthenticateRequest_IamToken{IamToken: token},
	}
}

func authorizeRequest(token string, checks []auth.AuthorizeCheck) *pb.AuthorizeRequest {
	authReq := pb.AuthorizeRequest{
		Checks: map[int64]*pb.AuthorizeCheck{},
	}
	for i, c := range checks {
		resources := make([]*pb.Resource, 0, len(c.ResourceID))
		for _, rid := range c.ResourceID {
			resources = append(resources, &pb.Resource{Id: rid})
		}

		authReq.Checks[int64(i)] = &pb.AuthorizeCheck{
			Identifier: &pb.AuthorizeCheck_IamToken{
				IamToken: token,
			},
			Permission:  &pb.Permission{Name: c.Permission},
			ContainerId: c.ContainerID,
			ResourcePath: &pb.ResourcePath{
				Path: resources,
			},
		}
	}
	return &authReq
}

var anonymousSubject = "{none}"

func accountToString(account *pb.Account) string {
	if account == nil {
		return ""
	}
	switch v := account.Type.(type) {
	case *pb.Account_UserAccount_:
		return v.UserAccount.Id
	case *pb.Account_ServiceAccount_:
		return v.ServiceAccount.Id
	case *pb.Account_AnonymousAccount_:
		return anonymousSubject
	default:
		return anonymousSubject
	}
}

func (p *authProviderNebius) MaskToken(token string) string {
	return strings.Split(token, ".")[0] + ".**"
}

func (p *authProviderNebius) GetYDBCPContainerID() string {
	return p.ydbcpContainerID
}

func (p *authProviderNebius) SetYDBCPContainerID(containerID string) {
	p.ydbcpContainerID = containerID
}

func processAuthorizeResponse(resp *pb.AuthorizeResponse, expectedResults int) ([]auth.AuthorizeResult, string, error) {
	if len(resp.Results) != expectedResults {
		return nil, "", fmt.Errorf(
			"access service unexpected respose results number %d, expected %d", len(resp.Results), expectedResults,
		)
	}
	results := make([]auth.AuthorizeResult, 0, expectedResults)
	var subject string
	for _, r := range resp.Results {
		authResult := auth.AuthorizeResult{}
		if len(subject) == 0 {
			subject = accountToString(r.Account)
		}
		switch r.ResultCode {
		case pb.AuthorizeResult_OK:
			authResult.Code = auth.AuthCodeSuccess
		case pb.AuthorizeResult_UNKNOWN_SUBJECT:
			authResult.Code = auth.AuthCodeUnknownSubject
			authResult.Message = r.Status.Message
		case pb.AuthorizeResult_INVALID_TOKEN:
			authResult.Code = auth.AuthCodeInvalidToken
			authResult.Message = r.Status.Message
		case pb.AuthorizeResult_PERMISSION_DENIED:
			authResult.Code = auth.AuthCodePermissionDenied
			authResult.Message = r.Status.Message
		default:
			authResult.Code = auth.AuthCodeError
			authResult.Message = "Unknown AuthorizeCode"
		}
		results = append(results, authResult)
	}
	return results, subject, nil
}

func (p *authProviderNebius) Authenticate(
	ctx context.Context, token string,
) (subject string, err error) {
	xlog.Info(
		ctx,
		"AuthProviderNebius authenticate",
	)
	if len(token) == 0 {
		xlog.Debug(ctx, "AuthProviderNebius got empty token")
		return anonymousSubject, nil
	}
	conn, err := p.client()
	if err != nil {
		xlog.Info(ctx, "access service client creation error", zap.Error(err))
		return anonymousSubject, err
	}
	defer conn.Close()
	client := pb.NewAccessServiceClient(conn)
	authReq := authenticateRequest(token)
	resp, err := client.Authenticate(ctx, authReq)
	if err != nil {
		xlog.Info(ctx, "access service client authentication error", zap.Error(err))
		return anonymousSubject, err
	}
	return accountToString(resp.Account), nil
}

func (p *authProviderNebius) Authorize(
	ctx context.Context,
	token string,
	checks ...auth.AuthorizeCheck,
) (results []auth.AuthorizeResult, subject string, err error) {
	xlog.Info(
		ctx,
		"AuthProviderNebius authorize",
		zap.String(log_keys.Checks, fmt.Sprintf("%v", checks)),
	)
	if len(token) == 0 {
		xlog.Debug(ctx, "AuthProviderNebius got empty token")
		results = make([]auth.AuthorizeResult, 0, len(checks))
		for range len(checks) {
			results = append(results, auth.AuthorizeResult{Code: auth.AuthCodeInvalidToken})
		}
	}
	if len(checks) == 0 {
		xlog.Info(ctx, "AuthorizeCheck list is empty")
		return nil, "", errors.New("AuthorizeCheck list is empty")
	}
	conn, err := p.client()
	if err != nil {
		xlog.Info(ctx, "access service client creation error", zap.Error(err))
		return nil, "", err
	}
	defer conn.Close()
	client := pb.NewAccessServiceClient(conn)
	authReq := authorizeRequest(token, checks)
	resp, err := client.Authorize(ctx, authReq)

	if err != nil {
		xlog.Error(ctx, "fail to call AccessService.Authorize", zap.Error(err))
		return nil, "", fmt.Errorf("access service call error: %w", err)
	}
	results, subject, err = processAuthorizeResponse(resp, len(checks))
	xlog.Info(
		ctx, "Authorize result",
		zap.String(log_keys.Results, fmt.Sprintf("%v", results)),
		zap.String(log_keys.Subject, subject),
		zap.Error(err),
	)
	return results, subject, err
}

func main() {}

var AuthProvider authProviderNebius
