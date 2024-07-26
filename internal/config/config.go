package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"ydbcp/internal/util/xlog"

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
}

type YDBConnectionConfig struct {
	ConnectionString   string `yaml:"connection_string"`
	Insecure           bool   `yaml:"insecure"`
	Discovery          bool   `yaml:"discovery" default:"true"`
	DialTimeoutSeconds uint32 `yaml:"dial_timeout_seconds" default:"5"`
}

type ClientConnectionConfig struct {
	Insecure           bool   `yaml:"insecure"`
	Discovery          bool   `yaml:"discovery" default:"true"`
	DialTimeoutSeconds uint32 `yaml:"dial_timeout_seconds" default:"5"`
}

type Config struct {
	DBConnection        YDBConnectionConfig    `yaml:"db_connection"`
	ClientConnection    ClientConnectionConfig `yaml:"client_connection"`
	S3                  S3Config               `yaml:"s3"`
	OperationTtlSeconds int64                  `yaml:"operation_ttl_seconds"`
}

func (config Config) ToString() (string, error) {
	data, err := yaml.Marshal(&config)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func InitConfig(ctx context.Context, confPath string) (Config, error) {
	if len(confPath) != 0 {
		confTxt, err := os.ReadFile(confPath)
		if err != nil {
			xlog.Error(ctx, "Unable to read configuration file",
				zap.String("config_path", confPath),
				zap.Error(err))
			return Config{}, err
		}
		var config Config
		err = yaml.Unmarshal(confTxt, &config)
		if err != nil {
			xlog.Error(ctx, "Unable to parse configuration file",
				zap.String("config_path", confPath),
				zap.Error(err))
			return Config{}, err
		}
		return config, nil
	}
	return Config{}, errors.New("configuration file path is empty")
}

func readSecret(filename string) (string, error) {
	rawSecret, err := os.ReadFile(filename)
	if err != nil {
		return "", fmt.Errorf("can't read file %s: %w", filename, err)
	}
	return strings.TrimSpace(string(rawSecret)), nil
}

func (c *S3Config) AccessKey() (string, error) {
	return readSecret(c.AccessKeyIDPath)
}

func (c *S3Config) SecretKey() (string, error) {
	return readSecret(c.SecretAccessKeyPath)

}
