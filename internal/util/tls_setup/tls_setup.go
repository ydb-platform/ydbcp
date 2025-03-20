package tls_setup

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"os"
)

func LoadTLSCredentials(RootCAPath *string, withInsecure bool) (grpc.DialOption, error) {
	var certPool *x509.CertPool
	if RootCAPath != nil && len(*RootCAPath) > 0 {
		caBundle, err := os.ReadFile(*RootCAPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read root ca bundle from file %s: %w", *RootCAPath, err)
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
	if withInsecure {
		tlsConfig.InsecureSkipVerify = true
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}
