package kms

import (
	"context"
	"fmt"

	"ydbcp/pkg/plugins/kms"
)

type MockKmsProvider struct {
	keys map[string][]byte
}

func NewMockKmsProvider(keys map[string][]byte) *MockKmsProvider {
	return &MockKmsProvider{
		keys: keys,
	}
}

func (p *MockKmsProvider) Init(_ context.Context, _ string) error {
	if p.keys == nil {
		p.keys = make(map[string][]byte)
	}
	return nil
}

func (p *MockKmsProvider) Close(_ context.Context) error {
	p.keys = nil
	return nil
}

func xor(data []byte, key []byte) []byte {
	out := make([]byte, len(data))
	for i := range data {
		out[i] = data[i] ^ key[i%len(key)]
	}
	return out
}

func (p *MockKmsProvider) Encrypt(
	_ context.Context,
	req *kms.EncryptRequest,
) (*kms.EncryptResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("mock kms: encrypt request is nil")
	}
	if len(req.Plaintext) == 0 {
		return &kms.EncryptResponse{KeyID: req.KeyID}, nil
	}

	key, ok := p.keys[req.KeyID]
	if !ok {
		return nil, fmt.Errorf("mock kms: key not found")
	}

	ciphertext := xor(req.Plaintext, key)
	return &kms.EncryptResponse{
		KeyID:      req.KeyID,
		Ciphertext: ciphertext,
	}, nil
}

func (p *MockKmsProvider) Decrypt(
	_ context.Context,
	req *kms.DecryptRequest,
) (*kms.DecryptResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("mock kms: decrypt request is nil")
	}
	if len(req.Ciphertext) == 0 {
		return &kms.DecryptResponse{KeyID: req.KeyID}, nil
	}

	key, ok := p.keys[req.KeyID]
	if !ok {
		return nil, fmt.Errorf("mock kms: key not found")
	}

	plaintext := xor(req.Ciphertext, key)
	return &kms.DecryptResponse{
		KeyID:     req.KeyID,
		Plaintext: plaintext,
	}, nil
}
