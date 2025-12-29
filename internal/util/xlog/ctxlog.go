package xlog

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"go.uber.org/zap"
)

var internalLogger atomic.Value

type loggerKey struct{}

func SetInternalLogger(logConfig *LogConfig) {
	logConfig.logger = logConfig.logger.WithOptions(zap.AddCallerSkip(1))
	internalLogger.Store(logConfig)
}

func Logger(ctx context.Context) *zap.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*zap.Logger); ok {
		return l
	}
	if l, ok := internalLogger.Load().(*LogConfig); ok {
		return l.logger
	}
	// Fallback, so we don't need to manually init logger in every test.
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.MessageKey = "message"
	SetInternalLogger(
		&LogConfig{
			logger: zap.Must(cfg.Build()),
			rawPrint: func(msg string) {
				fmt.Fprintln(os.Stdout, msg)
			},
		},
	)
	return Logger(ctx)
}

func With(ctx context.Context, fields ...zap.Field) context.Context {
	return context.WithValue(ctx, loggerKey{}, Logger(ctx).With(fields...))
}

func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).Debug(msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).Info(msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).Warn(msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).Fatal(msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).Error(msg, fields...)
}

func Raw(msg string) {
	if l, ok := internalLogger.Load().(*LogConfig); ok {
		l.rawPrint(msg)
	} else {
		fmt.Fprintln(os.Stdout, msg)
	}
}
