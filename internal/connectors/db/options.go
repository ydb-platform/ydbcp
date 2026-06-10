package db

import (
	"fmt"
	"time"

	ydbPrometheus "github.com/ydb-platform/ydb-go-sdk-prometheus/v2"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"

	"ydbcp/internal/config"
	"ydbcp/internal/credentials"
	"ydbcp/internal/metrics"
)

func YdbOptionsFromConfig(cfg config.YDBConnectionConfig, enableSDKMetrics bool) ([]ydb.Option, error) {
	dialTimeout := time.Second * time.Duration(cfg.DialTimeoutSeconds)
	opts := []ydb.Option{
		ydb.WithDialTimeout(dialTimeout),
	}
	if cfg.Insecure {
		opts = append(opts, ydb.WithTLSSInsecureSkipVerify())
	}
	if !cfg.Discovery {
		opts = append(opts, ydb.WithBalancer(balancers.SingleConn()))
	}
	if cfg.K8sJWTAuth != nil {
		creds, err := credentials.NewK8sJWTCredentials(credentials.K8sJWTConfig{
			K8sTokenPath:         cfg.K8sJWTAuth.K8sTokenPath,
			TokenServiceEndpoint: cfg.K8sJWTAuth.TokenServiceEndpoint,
			SubjectToken:         cfg.K8sJWTAuth.SubjectToken,
			SubjectTokenType:     cfg.K8sJWTAuth.SubjectTokenType,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create K8s JWT credentials: %w", err)
		}
		opts = append(opts, ydb.WithCredentials(creds))
	} else if len(cfg.OAuth2KeyFile) > 0 {
		opts = append(opts, ydb.WithOauth2TokenExchangeCredentialsFile(cfg.OAuth2KeyFile))
	} else {
		opts = append(opts, ydb.WithAnonymousCredentials())
	}
	if enableSDKMetrics {
		opts = append(opts, ydbPrometheus.WithTraces(metrics.GlobalMetricsRegistry.GetReg()))
	}
	return opts, nil
}
