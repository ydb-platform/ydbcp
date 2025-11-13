package kms

import "context"

type EncryptRequest struct {
	KeyID     string
	Plaintext []byte
}

type EncryptResponse struct {
	KeyID      string
	Ciphertext []byte
}

type DecryptRequest struct {
	KeyID      string
	Ciphertext []byte
}

type DecryptResponse struct {
	KeyID     string
	Plaintext []byte
}

// KmsProvider is an interface that KMS plugins must implement.
type KmsProvider interface {
	Init(ctx context.Context, config string) error
	Close(ctx context.Context) error

	Encrypt(ctx context.Context, req *EncryptRequest) (*EncryptResponse, error)
	Decrypt(ctx context.Context, req *DecryptRequest) (*DecryptResponse, error)
}
