package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/auth"
	pb "ydbcp/plugins/auth_nebius/proto/iam/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

type authProviderNebius struct {
	endpoint string
	tls      bool
	config   pluginConfig
}

type pluginConfig struct {
	AccessServiceEndpoint string `yaml:"access_service_endpoint"`
	Insecure              bool   `yaml:"insecure" default:"false"`
	RootCAPath            string `yaml:"root_ca_path"`
}

func parseEndpoint(e string) (string, bool) {
	if strings.HasPrefix(e, "grpcs://") {
		return e[8:], true
	}
	if strings.HasPrefix(e, "grpc://") {
		return e[7:], false
	}
	return e, false
}

func (p *authProviderNebius) loadTLSCredentials() (grpc.DialOption, error) {
	if !p.tls {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	var certPool *x509.CertPool
	if len(p.config.RootCAPath) > 0 {
		caBundle, err := os.ReadFile(p.config.RootCAPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read root ca bundle from file %s: %w", p.config.RootCAPath, err)
		}
		certPool = x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(caBundle); !ok {
			return nil, errors.New("failed to parse CA bundle")
		}
	} else {
		var err error
		certPool, err = x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to get system cert pool: %w", err)
		}
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    certPool,
	}
	if p.config.Insecure {
		tlsConfig.InsecureSkipVerify = true
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}

func (p *authProviderNebius) Init(ctx context.Context, config string) error {
	xlog.Info(ctx, "AuthProviderNebius init", zap.String("config", config))
	err := yaml.Unmarshal([]byte(config), &p.config)
	if err != nil {
		xlog.Error(ctx, "Unable to parse configuration file", zap.Error(err))
		return fmt.Errorf("unable to parse configuration %w", err)
	}
	p.endpoint, p.tls = parseEndpoint(p.config.AccessServiceEndpoint)
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

func accountToString(account *pb.Account) string {
	switch v := account.Type.(type) {
	case *pb.Account_UserAccount_:
		return v.UserAccount.Id
	case *pb.Account_ServiceAccount_:
		return v.ServiceAccount.Id
	case *pb.Account_AnonymousAccount_:
		return "anonymous"
	default:
		return "unknown"
	}
}

func processAuthorizeResponse(resp *pb.AuthorizeResponse, expectedResults int) ([]auth.AuthorizeResult, string, error) {
	if len(resp.Results) != expectedResults {
		return nil, "", fmt.Errorf("access service unexpected respose results number %d, expected %d", len(resp.Results), expectedResults)
	}
	results := make([]auth.AuthorizeResult, 0, expectedResults)
	var subject string
	for _, r := range resp.Results {
		authResult := auth.AuthorizeResult{}
		switch r.ResultCode {
		case pb.AuthorizeResult_OK:
			authResult.Code = auth.AuthCodeSuccess
			if len(subject) == 0 {
				subject = accountToString(r.Account)
			}
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
			authResult.Message = fmt.Sprintf("Unknown AuthorizeCode")
		}
		results = append(results, authResult)
	}
	return results, subject, nil
}

func (p *authProviderNebius) Authorize(
	ctx context.Context,
	token string,
	checks ...auth.AuthorizeCheck,
) (results []auth.AuthorizeResult, subject string, err error) {
	xlog.Info(
		ctx,
		"AuthProviderNebius authorize",
		zap.String("token", auth.MaskToken(token)),
		zap.String("checks", fmt.Sprintf("%v", checks)),
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
	xlog.Info(ctx, "Authorize result",
		zap.String("results", fmt.Sprintf("%v", results)),
		zap.String("subject", subject),
		zap.Error(err),
	)
	return results, subject, err
}

func main() {}

var AuthProvider authProviderNebius
