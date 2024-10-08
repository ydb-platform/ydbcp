package xlog

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func SetupLogging(verbose bool) *zap.Logger {
	var level zapcore.Level = zapcore.WarnLevel
	if verbose {
		level = zapcore.DebugLevel
	}
	cfg := zap.NewProductionConfig()
	cfg.Level.SetLevel(level)
	cfg.EncoderConfig.MessageKey = "message"

	l, err := cfg.Build()
	if err != nil {
		log.Fatalln("Failed to create logger:", err)
	}
	return l
}
