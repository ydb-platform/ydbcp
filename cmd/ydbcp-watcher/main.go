package main

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Cms_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Cms"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"ydbcp/cmd/integration/common"
	"ydbcp/internal/config"
	"ydbcp/internal/metrics"
	"ydbcp/internal/util/tls_setup"
	"ydbcp/internal/util/xlog"
	pb "ydbcp/pkg/proto/ydbcp/v1alpha1"
)

type Oauth2Config credentials.OAuth2Config

func (c Oauth2Config) Validate() error {
	emp := Oauth2Config{}
	if c == emp {
		return errors.New("empty config")
	}
	return nil
}

type tokenResponse struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	ExpiresIn   int64     `json:"expires_in"`
	Scope       string    `json:"scope"`
	Now         time.Time `json:"-"`
}

func validateTokenResponse(parsedResponse *tokenResponse) error {
	if !strings.EqualFold(parsedResponse.TokenType, "bearer") {
		return errors.New("invalid token type")
	}

	if parsedResponse.ExpiresIn <= 0 {
		return errors.New("invalid token expires_in")
	}

	if parsedResponse.AccessToken == "" {
		return errors.New("access token is empty")
	}

	return nil
}

func processTokenExchangeResponse(
	result *http.Response,
	now time.Time,
) (*tokenResponse, error) {
	data, err := io.ReadAll(result.Body)

	if err != nil {
		return nil, err
	}

	if result.StatusCode != http.StatusOK {
		return nil, errors.New("status " + result.Status + "for response " + string(data))
	}

	var parsedResponse tokenResponse
	if err = json.Unmarshal(data, &parsedResponse); err != nil {
		return nil, err
	}

	if err := validateTokenResponse(&parsedResponse); err != nil {
		return nil, err
	}

	parsedResponse.Now = now

	return &parsedResponse, nil
}

func getRequestParams(oauth2 *Oauth2Config, token string) string {
	params := url.Values{}
	params.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
	if oauth2.Resource != nil {
		for _, res := range oauth2.Resource.Values {
			if res != "" {
				params.Add("resource", res)
			}
		}
	}

	if oauth2.SubjectCreds != nil && oauth2.SubjectCreds.Audience != nil {
		for _, aud := range oauth2.SubjectCreds.Audience.Values {
			if aud != "" {
				params.Add("audience", aud)
			}
		}
	}

	params.Set("requested_token_type", "urn:ietf:params:oauth:token-type:access_token")
	params.Set("subject_token", token)
	params.Set("subject_token_type", "urn:ietf:params:oauth:token-type:jwt")

	return params.Encode()
}

func performExchangeTokenRequest(ctx context.Context, oauth2 *Oauth2Config, token string) (resp *tokenResponse, final error) {
	now := time.Now()

	body := getRequestParams(oauth2, token)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, oauth2.TokenEndpoint, strings.NewReader(body))
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
		} else {
			break
		}
	}

	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, errors.New("empty response")
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			final = err
		}
	}(result.Body)

	return processTokenExchangeResponse(result, now)
}

var ydbToken atomic.Pointer[string]
var ydbcpToken atomic.Pointer[string]

func TokenInterceptor(header string, tokenPtr *atomic.Pointer[string]) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		token := tokenPtr.Load()
		if token != nil {
			ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
				header: *token,
			}))
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func getToken(ctx context.Context, key *rsa.PrivateKey, oauth2 *Oauth2Config, pointer *atomic.Pointer[string]) {
	claims := jwt.RegisteredClaims{
		Issuer:    oauth2.SubjectCreds.Issuer,
		Subject:   oauth2.SubjectCreds.Subject,
		Audience:  oauth2.SubjectCreds.Audience.Values,
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = oauth2.SubjectCreds.KeyID
	signedToken, err := token.SignedString(key)
	if err != nil {
		xlog.Error(ctx, "Failed to sign token", zap.Error(err))
	}

	result, err := performExchangeTokenRequest(ctx, oauth2, signedToken)
	if err != nil {
		xlog.Error(ctx, "Failed to perform token exchange", zap.Error(err))
	} else {
		newToken := "Bearer " + result.AccessToken
		xlog.Info(ctx, "Token exchanged")
		pointer.Store(&newToken)
	}
}

func StartCredentialsHelper(ctx context.Context, wg *sync.WaitGroup, keyFile string, tokenPtr *atomic.Pointer[string]) {
	oauth2, _ := config.InitConfig[Oauth2Config](ctx, keyFile)

	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(oauth2.SubjectCreds.PrivateKey))
	if err != nil {
		xlog.Error(ctx, "Failed to parse private key", zap.Error(err))
		os.Exit(1)
	}

	getToken(ctx, key, oauth2, tokenPtr)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(55 * time.Minute)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				getToken(ctx, key, oauth2, tokenPtr)
			}
		}
	}()
}

func GetSchedulesFromYDBCP(ctx context.Context, scheduleClient pb.BackupScheduleServiceClient) map[string]bool {
	schedules := make(map[string]bool)
	statusFilter := []pb.BackupSchedule_Status{pb.BackupSchedule_ACTIVE}
	pageToken := ""
	for {
		pbSchedules, err := scheduleClient.ListBackupSchedules(
			ctx, &pb.ListBackupSchedulesRequest{
				DatabaseNameMask: "%",
				DisplayStatus:    statusFilter,
				PageSize:         1000,
				PageToken:        pageToken,
			},
		)
		if err != nil {
			xlog.Error(ctx, "Error listing backup schedules", zap.Error(err))
		} else {
			xlog.Info(ctx, "Got schedules from YDBCP", zap.String("proto", pbSchedules.String()))
			for _, pbSchedule := range pbSchedules.Schedules {
				schedules[pbSchedule.DatabaseName] = true
			}
			if pbSchedules.NextPageToken == "" {
				break
			}
			pageToken = pbSchedules.NextPageToken
		}
	}
	return schedules
}

func main() {
	var confPath string

	flag.StringVar(
		&confPath, "config", "config.yaml", "configuration file",
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configInstance, err := config.InitConfig[config.WatcherConfig](ctx, confPath)

	if err != nil {
		log.Error(fmt.Errorf("unable to initialize config: %w", err))
		os.Exit(1)
	}

	var wg sync.WaitGroup

	logger, err := xlog.SetupLogging(zap.InfoLevel.String())
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	xlog.SetInternalLogger(logger)
	defer func() {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("Failed to sync logger: %s\n", err)
		}
	}()

	_, err = maxprocs.Set(maxprocs.Logger(func(f string, p ...interface{}) { xlog.Info(ctx, fmt.Sprintf(f, p...)) }))
	if err != nil {
		xlog.Error(ctx, "Can't set maxprocs", zap.Error(err))
	}

	if confStr, err := configInstance.ToString(); err == nil {
		xlog.Debug(
			ctx, "Use configuration file",
			zap.String("ConfigPath", confPath),
			zap.String("config", confStr),
		)
	}

	opt, err := tls_setup.LoadTLSCredentials(nil, false)
	if err != nil {
		xlog.Error(ctx, "Failed to setup TLS", zap.Error(err))
		os.Exit(1)
	}

	StartCredentialsHelper(ctx, &wg, configInstance.ClusterConnection.OAuth2KeyFile, &ydbToken)
	StartCredentialsHelper(ctx, &wg, configInstance.ControlPlaneConnection.OAuth2KeyFile, &ydbcpToken)

	var opts []grpc.DialOption
	opts = append(opts, opt)
	opts = append(opts, grpc.WithUnaryInterceptor(TokenInterceptor("authorization", &ydbcpToken)))
	conn := common.CreateGRPCClientWithOpts(configInstance.ControlPlaneConnection.Endpoint, opts)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			xlog.Error(ctx, "failed to close connection")
		}
	}(conn)
	scheduleClient := pb.NewBackupScheduleServiceClient(conn)

	//connect to YDB cluster
	opts = []grpc.DialOption{opt}
	opts = append(opts, grpc.WithUnaryInterceptor(TokenInterceptor("x-ydb-auth-ticket", &ydbToken)))
	ydb := common.CreateGRPCClientWithOpts(configInstance.ClusterConnection.Endpoint, opts)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			xlog.Error(ctx, "failed to close connection")
		}
	}(ydb)

	xlog.Info(ctx, "created cluster GRPCConn", zap.String("endpoint", configInstance.ClusterConnection.Endpoint))
	cmsService := Ydb_Cms_V1.NewCmsServiceClient(ydb)

	dbExceptions := make(map[string]bool, len(configInstance.DBExceptions))
	for _, database := range configInstance.DBExceptions {
		dbExceptions[database] = true
	}

	reg := prometheus.NewRegistry()

	noScheduleDbs := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "healthcheck",
		Name:      "no_schedule_dbs",
		Help:      "Number of databases with no schedules on a cluster",
	}, []string{})

	_ = metrics.CreateMetricsServer(ctx, &wg, reg, &configInstance.MetricsServer)

	xlog.Info(ctx, "YDBCP watcher started")

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	wg.Add(1)
	go func() {
		defer wg.Done()
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-ctx.Done():
			return
		case sig := <-sigs:
			xlog.Info(ctx, "got signal", zap.String("signal", sig.String()))
			cancel()
		}
	}()

LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case <-ticker.C:
			{
				cnt := 0
				schedules := GetSchedulesFromYDBCP(ctx, scheduleClient)

				op, err := cmsService.ListDatabases(ctx, &Ydb_Cms.ListDatabasesRequest{
					OperationParams: &Ydb_Operations.OperationParams{
						OperationTimeout: durationpb.New(time.Second * 10),
					},
				})
				if err != nil {
					xlog.Error(ctx, "Error listing databases", zap.Error(err))
					continue
				}
				var databases Ydb_Cms.ListDatabasesResult
				err = op.Operation.Result.UnmarshalTo(&databases)
				if err != nil {
					xlog.Error(ctx, "Error unmarshalling databases", zap.Error(err))
					continue
				}
				xlog.Info(ctx, "got databases from cluster", zap.Strings("databases", databases.Paths))

				for _, fullName := range databases.Paths {
					if dbExceptions[fullName] {
						continue
					}
					if !schedules[fullName] {
						xlog.Error(ctx, "database has no schedule", zap.String("database_name", fullName))
						cnt++
					}
				}
				noScheduleDbs.WithLabelValues().Set(float64(cnt))
			}
		}
	}

	wg.Wait()
}
