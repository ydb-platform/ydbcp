package xlog

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"log"
	"os"
)

func rawPrint(ws zapcore.WriteSyncer) func(msg string) {
	rawEncoderCfg := zap.NewProductionEncoderConfig()
	rawEncoderCfg.TimeKey = ""
	rawEncoderCfg.LevelKey = ""
	rawEncoderCfg.NameKey = ""
	rawEncoderCfg.CallerKey = ""
	rawEncoder := zapcore.NewConsoleEncoder(rawEncoderCfg)
	rawCore := zapcore.NewCore(rawEncoder, ws, zapcore.DebugLevel)
	rawLogger := zap.New(rawCore)

	return func(msg string) {
		rawLogger.Info(msg)
	}
}

type LogConfig struct {
	logger   *zap.Logger
	rawPrint func(msg string)
}

func (c *LogConfig) Sync() error {
	return c.logger.Sync()
}

func SetupLogging(logLevel string) (*LogConfig, error) {
	level, err := zapcore.ParseLevel(logLevel)
	if err != nil {
		return nil, fmt.Errorf("parse log level error: %w", err)
	}
	cfg := zap.NewProductionConfig()
	cfg.Level.SetLevel(level)
	cfg.EncoderConfig.MessageKey = "message"

	l, err := cfg.Build()
	if err != nil {
		log.Fatalln("Failed to create logger:", err)
	}

	return &LogConfig{
		logger:   l,
		rawPrint: func(msg string) { fmt.Fprintln(os.Stdout, msg) },
	}, nil
}

func SetupLoggingWithFile(logLevel string, logFile string) (*LogConfig, error) {
	level, err := zapcore.ParseLevel(logLevel)
	if err != nil {
		return nil, fmt.Errorf("parse log level error: %w", err)
	}
	if logFile == "" {
		return nil, fmt.Errorf("log file path is empty")
	}
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	multiWriter := io.MultiWriter(os.Stdout, f)
	ws := zapcore.AddSync(multiWriter)

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.MessageKey = "message"
	encoder := zapcore.NewJSONEncoder(encoderCfg)

	core := zapcore.NewCore(encoder, ws, level)

	logger := zap.New(core)
	return &LogConfig{
		logger:   logger,
		rawPrint: rawPrint(ws),
	}, nil
}
