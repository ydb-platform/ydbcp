package main

import (
	"context"
	"fmt"

	"ydbcp/internal/util/tls_setup"
	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/kms"
	pb "ydbcp/plugins/kms_nebius/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

type kmsProviderNebius struct {
	endpoint string
	config   pluginConfig
}

type pluginConfig struct {
	CryptoServiceEndpoint string `yaml:"crypto_service_endpoint"`
	Insecure              bool   `yaml:"insecure" default:"false"`
	RootCAPath            string `yaml:"root_ca_path"`
	ClientKeyPath         string `yaml:"client_key_path"`
	ClientCertificatePath string `yaml:"client_certificate_path"`
}

func (p *kmsProviderNebius) Init(ctx context.Context, rawConfig string) error {
	xlog.Info(ctx, "KmsNebiusProvider initialization started", zap.String("config", rawConfig))
	if err := yaml.Unmarshal([]byte(rawConfig), &p.config); err != nil {
		xlog.Error(ctx, "Unable to parse configuration", zap.Error(err))
		return fmt.Errorf("kms: unable to parse configuration: %w", err)
	}
	if len(p.config.CryptoServiceEndpoint) == 0 {
		return fmt.Errorf("kms: crypto service endpoint is required in configuration")
	}
	if len(p.config.RootCAPath) == 0 {
		return fmt.Errorf("kms: root ca path is required in configuration")
	}
	if len(p.config.ClientKeyPath) == 0 {
		return fmt.Errorf("kms: client key path is required in configuration")
	}
	if len(p.config.ClientCertificatePath) == 0 {
		return fmt.Errorf("kms: client certificate path is required in configuration")
	}

	p.endpoint, _ = tls_setup.ParseEndpoint(p.config.CryptoServiceEndpoint)

	xlog.Info(ctx, "KmsNebiusProvider was initialized successfully")
	return nil
}

func (p *kmsProviderNebius) Close(ctx context.Context) error {
	xlog.Info(ctx, "KmsNebiusProvider was closed")
	return nil
}

func (p *kmsProviderNebius) Encrypt(ctx context.Context, req *kms.EncryptRequest) (*kms.EncryptResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("kms: encryption request is nil")
	}
	if len(req.KeyID) == 0 {
		return nil, fmt.Errorf("kms: key id is required in encryption request")
	}

	tlsOption, err := tls_setup.LoadMTLSCredentials(&p.config.RootCAPath, &p.config.ClientCertificatePath, &p.config.ClientKeyPath, p.config.Insecure)
	if err != nil {
		return nil, err
	}

	grpcClient, err := grpc.NewClient("dns:"+p.endpoint, tlsOption)
	if err != nil {
		return nil, err
	}
	defer grpcClient.Close()

	client := pb.NewSymmetricCryptoServiceClient(grpcClient)
	resp, err := client.Encrypt(ctx, &pb.SymmetricEncryptRequest{
		KeyId:     req.KeyID,
		Plaintext: req.Plaintext,
	})
	if err != nil {
		return nil, fmt.Errorf("kms: encryption failed: %w", err)
	}

	return &kms.EncryptResponse{
		KeyID:      resp.GetKeyId(),
		Ciphertext: resp.GetCiphertext(),
	}, nil
}

func (p *kmsProviderNebius) Decrypt(ctx context.Context, req *kms.DecryptRequest) (*kms.DecryptResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("kms: decryption request is nil")
	}
	if len(req.KeyID) == 0 {
		return nil, fmt.Errorf("kms: key id is required in decryption request")
	}

	tlsOption, err := tls_setup.LoadTLSCredentials(&p.config.RootCAPath, p.config.Insecure)
	if err != nil {
		return nil, err
	}

	grpcClient, err := grpc.NewClient("dns:"+p.endpoint, tlsOption)
	if err != nil {
		return nil, err
	}
	defer grpcClient.Close()

	client := pb.NewSymmetricCryptoServiceClient(grpcClient)
	resp, err := client.Decrypt(ctx, &pb.SymmetricDecryptRequest{
		KeyId:      req.KeyID,
		Ciphertext: req.Ciphertext,
	})
	if err != nil {
		return nil, fmt.Errorf("kms: decryption was failed: %w", err)
	}

	return &kms.DecryptResponse{
		KeyID:     resp.GetKeyId(),
		Plaintext: resp.GetPlaintext(),
	}, nil
}

func main() {}

var KmsProvider kmsProviderNebius
