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

type Config struct {
	YdbcpDbConnectionString string   `yaml:"ydbcp_db_connection_string"`
	S3                      S3Config `yaml:"s3"`
}

func (c *Config) ToString() (string, error) {
	data, err := yaml.Marshal(&c)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func NewConfig(ctx context.Context, confPath string) (*Config, error) {
	if len(confPath) == 0 {
		return nil, errors.New("configuration file path is empty")
	}
	confTxt, err := os.ReadFile(confPath)
	if err != nil {
		xlog.Error(ctx, "Unable to read configuration file",
			zap.String("config_path", confPath),
			zap.Error(err))
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(confTxt, &config)
	if err != nil {
		xlog.Error(ctx, "Unable to parse configuration file",
			zap.String("config_path", confPath),
			zap.Error(err))
		return nil, err
	}
	return &config, nil
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
