package tls_setup

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func ParseEndpoint(e string) (string, bool) {
	if strings.HasPrefix(e, "grpcs://") {
		return e[8:], true
	}
	if strings.HasPrefix(e, "grpc://") {
		return e[7:], false
	}
	return e, false
}

func LoadTLSCredentials(rootCAPath *string, withInsecure bool) (grpc.DialOption, error) {
	tlsConfig, err := buildTLSConfig(rootCAPath, withInsecure)
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}

func LoadMTLSCredentials(
	rootCAPath *string,
	clientCertPath *string,
	clientKeyPath *string,
	withInsecure bool,
) (grpc.DialOption, error) {
	if clientCertPath == nil || len(*clientCertPath) == 0 {
		return nil, fmt.Errorf("client certificate path is required for mTLS")
	}
	if clientKeyPath == nil || len(*clientKeyPath) == 0 {
		return nil, fmt.Errorf("client key path is required for mTLS")
	}

	tlsConfig, err := buildTLSConfig(rootCAPath, withInsecure)
	if err != nil {
		return nil, err
	}

	cert, err := tls.LoadX509KeyPair(*clientCertPath, *clientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate/key: %w", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}

func buildTLSConfig(rootCAPath *string, withInsecure bool) (*tls.Config, error) {
	var certPool *x509.CertPool
	if rootCAPath != nil && len(*rootCAPath) > 0 {
		caBundle, err := os.ReadFile(*rootCAPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read root ca bundle from file %s: %w", *rootCAPath, err)
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
	return tlsConfig, nil
}
