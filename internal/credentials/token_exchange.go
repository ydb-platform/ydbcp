package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"ydbcp/internal/config"
)

const (
	grantTypeTokenExchange   = "urn:ietf:params:oauth:grant-type:token-exchange"
	requestedTokenTypeAccess = "urn:ietf:params:oauth:token-type:access_token"
)

type tokenExchangeResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
}

func GetK8sJWTToken(ctx context.Context, cfg *config.K8sJWTAuthConfig) (string, error) {
	if cfg == nil {
		return "", errors.New("k8s_jwt_auth is required")
	}

	k8sToken, err := os.ReadFile(cfg.K8sTokenPath)
	if err != nil {
		return "", fmt.Errorf("failed to read K8s token from %s: %w", cfg.K8sTokenPath, err)
	}

	body := getK8sJWTRequestParams(cfg, strings.TrimSpace(string(k8sToken)))
	resp, err := performExchangeTokenRequest(ctx, cfg.TokenServiceEndpoint, body)
	if err != nil {
		return "", err
	}

	return "Bearer " + resp.AccessToken, nil
}

func getK8sJWTRequestParams(cfg *config.K8sJWTAuthConfig, actorToken string) string {
	params := url.Values{}
	params.Set("grant_type", grantTypeTokenExchange)
	params.Set("requested_token_type", requestedTokenTypeAccess)
	params.Set("actor_token", actorToken)
	params.Set("actor_token_type", JWTTokenType)
	params.Set("subject_token", cfg.SubjectToken)
	params.Set("subject_token_type", cfg.SubjectTokenType)

	return params.Encode()
}

func performExchangeTokenRequest(ctx context.Context, endpoint, body string) (*tokenExchangeResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(body)))
	req.Close = true

	client := http.Client{
		Transport: http.DefaultTransport,
		Timeout:   time.Second * 10,
	}

	var result *http.Response
	for range 5 {
		result, err = client.Do(req)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("empty token exchange response")
	}
	defer func() {
		_ = result.Body.Close()
	}()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	if result.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token exchange failed: status %s, body %s", result.Status, string(data))
	}

	var parsed tokenExchangeResponse
	if err = json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}

	if !strings.EqualFold(parsed.TokenType, "bearer") {
		return nil, errors.New("token exchange returned invalid token type")
	}
	if parsed.ExpiresIn <= 0 {
		return nil, errors.New("token exchange returned invalid expires_in")
	}
	if strings.TrimSpace(parsed.AccessToken) == "" {
		return nil, errors.New("token exchange returned empty access token")
	}

	return &parsed, nil
}
