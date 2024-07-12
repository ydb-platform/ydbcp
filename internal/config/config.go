package config

import (
	"context"
	"errors"
	"os"
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
