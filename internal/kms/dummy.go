package kms

import (
	"context"
	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/kms"

	"go.uber.org/zap"
)

type kmsProviderDummy struct {
}

func (p *kmsProviderDummy) Init(ctx context.Context, _ string) error {
	xlog.Info(ctx, "KmsProviderDummy was initialized successfully")
	return nil
}

func (p *kmsProviderDummy) Close(ctx context.Context) error {
	xlog.Info(ctx, "KmsProviderDummy was closed")
	return nil
}

func (p *kmsProviderDummy) Encrypt(
	ctx context.Context,
	req *kms.EncryptRequest,
) (*kms.EncryptResponse, error) {
	xlog.Info(
		ctx,
		"KmsProviderDummy Encrypt",
		zap.String("KeyID", req.KeyID),
	)

	return &kms.EncryptResponse{
		KeyID:      req.KeyID,
		Ciphertext: req.Plaintext,
	}, nil
}

func (p *kmsProviderDummy) Decrypt(
	ctx context.Context,
	req *kms.DecryptRequest,
) (*kms.DecryptResponse, error) {
	xlog.Info(
		ctx,
		"KmsProviderDummy Decrypt",
		zap.String("KeyID", req.KeyID),
	)

	return &kms.DecryptResponse{
		KeyID:     req.KeyID,
		Plaintext: req.Ciphertext,
	}, nil
}

func NewDummyKmsProvider(ctx context.Context) (kms.KmsProvider, error) {
	p := &kmsProviderDummy{}
	if err := p.Init(ctx, ""); err != nil {
		return nil, err
	}
	return p, nil
}
