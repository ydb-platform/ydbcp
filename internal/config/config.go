package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"ydbcp/internal/util/xlog"

	"github.com/creasty/defaults"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type S3Config struct {
	Endpoint            string `yaml:"endpoint"`
	Region              string `yaml:"region"`
	Bucket              string `yaml:"bucket"`
	PathPrefix          string `yaml:"path_prefix"`
	AccessKeyIDPath     string `yaml:"access_key_id_path"`
	SecretAccessKeyPath string `yaml:"secret_access_key_path"`
	S3ForcePathStyle    bool   `yaml:"s3_force_path_style"`
	IsMock              bool
}

type YDBConnectionConfig struct {
	ConnectionString   string `yaml:"connection_string"`
	Insecure           bool   `yaml:"insecure"`
	Discovery          bool   `yaml:"discovery" default:"true"`
	DialTimeoutSeconds uint32 `yaml:"dial_timeout_seconds" default:"5"`
	OAuth2KeyFile      string `yaml:"oauth2_key_file"`
	EnableSDKMetrics   bool   `yaml:"enable_sdk_metrics"`
}

type ClientConnectionConfig struct {
	Insecure               bool     `yaml:"insecure"`
	Discovery              bool     `yaml:"discovery" default:"true"`
	DialTimeoutSeconds     uint32   `yaml:"dial_timeout_seconds" default:"5"`
	OAuth2KeyFile          string   `yaml:"oauth2_key_file"`
	AllowedEndpointDomains []string `yaml:"allowed_endpoint_domains"`
	AllowInsecureEndpoint  bool     `yaml:"allow_insecure_endpoint"`
}

type AuthConfig struct {
	PluginPath    string      `yaml:"plugin_path"`
	Configuration interface{} `yaml:"configuration"`
}

type GRPCServerConfig struct {
	BindAddress        string `yaml:"bind_address"`
	BindPort           uint16 `yaml:"bind_port" default:"2135"`
	TLSCertificatePath string `yaml:"tls_certificate_path"`
	TLSKeyPath         string `yaml:"tls_key_path"`
	LogLevel           string `yaml:"log_level"`
}

type MetricsServerConfig struct {
	BindAddress        string `yaml:"bind_address"`
	BindPort           uint16 `yaml:"bind_port" default:"8080"`
	TLSCertificatePath string `yaml:"tls_certificate_path"`
	TLSKeyPath         string `yaml:"tls_key_path"`
}

type Config struct {
	DBConnection             YDBConnectionConfig    `yaml:"db_connection"`
	ClientConnection         ClientConnectionConfig `yaml:"client_connection"`
	S3                       S3Config               `yaml:"s3"`
	OperationTtlSeconds      int64                  `yaml:"operation_ttl_seconds"`
	Auth                     AuthConfig             `yaml:"auth"`
	GRPCServer               GRPCServerConfig       `yaml:"grpc_server"`
	MetricsServer            MetricsServerConfig    `yaml:"metrics_server"`
	SchedulesLimitPerDB      int                    `yaml:"schedules_limit_per_db" default:"10"`
	ProcessorIntervalSeconds int64                  `yaml:"processor_interval_seconds" default:"10"`
}

var (
	validDomainFilter = regexp.MustCompile(`^[A-Za-z\.][A-Za-z0-9\-\.]+[A-Za-z]$`)
)

func (config Config) ToString() (string, error) {
	data, err := yaml.Marshal(&config)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func InitConfig(ctx context.Context, confPath string) (Config, error) {
	if len(confPath) == 0 {
		return Config{}, errors.New("configuration file path is empty")
	}
	ctx = xlog.With(ctx, zap.String("ConfigPath", confPath))
	confTxt, err := os.ReadFile(confPath)
	if err != nil {
		xlog.Error(ctx, "Unable to read configuration file", zap.Error(err))
		return Config{}, err
	}
	confTxt = []byte(os.ExpandEnv(string(confTxt)))
	var config Config
	if err = defaults.Set(&config); err != nil {
		xlog.Error(ctx, "Unable to set default configuration parameters", zap.Error(err))
		return Config{}, err
	}

	err = yaml.Unmarshal(confTxt, &config)
	if err != nil {
		xlog.Error(ctx, "Unable to parse configuration file", zap.Error(err))
		return Config{}, err
	}
	return config, config.Validate()
}

func (c *Config) Validate() error {
	for _, domain := range c.ClientConnection.AllowedEndpointDomains {
		if !validDomainFilter.MatchString(domain) {
			return fmt.Errorf("incorrect domain filter in allowed_endpoint_domains: %s", domain)
		}
	}
	return nil
}

func readSecret(filename string) (string, error) {
	rawSecret, err := os.ReadFile(filename)
	if err != nil {
		return "", fmt.Errorf("can't read file %s: %w", filename, err)
	}
	return strings.TrimSpace(string(rawSecret)), nil
}

func (c *S3Config) AccessKey() (string, error) {
	if c.IsMock {
		return "", nil
	}
	return readSecret(c.AccessKeyIDPath)
}

func (c *S3Config) SecretKey() (string, error) {
	if c.IsMock {
		return "", nil
	}
	return readSecret(c.SecretAccessKeyPath)

}

func (c *AuthConfig) ConfigurationString() (string, error) {
	txt, err := yaml.Marshal(c.Configuration)
	if err != nil {
		return "", fmt.Errorf("can't marshal Auth.Configuration to YAML: %w", err)
	}
	return string(txt), nil
}
