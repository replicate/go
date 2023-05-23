package logging

import (
	"context"
	"net/http"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	baseConfig = NewConfig()
	baseLogger = zap.Must(baseConfig.Build())
)

type contextKey int

const (
	contextFieldsKey contextKey = iota
)

func NewConfig() zap.Config {
	var config zap.Config

	development := os.Getenv("LOG_FORMAT") == "development"

	if development {
		config = newDevelopmentConfig()
	} else {
		config = newProductionConfig()
	}

	level, ok := os.LookupEnv("LOG_LEVEL")
	if ok {
		// Temporarily treat "warning" like "warn" for backwards compatibility with
		// logrus.
		if strings.ToLower(level) == "warning" {
			level = "warn"
		}

		if lvl, err := zap.ParseAtomicLevel(level); err == nil {
			config.Level = lvl
		}
	}

	return config
}

func newDevelopmentConfig() zap.Config {
	return zap.Config{
		Level:             zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:       true,
		DisableStacktrace: true,
		Encoding:          "console",
		EncoderConfig:     newDevelopmentEncoderConfig(),
		OutputPaths:       []string{"stderr"},
	}
}

func newProductionConfig() zap.Config {
	return zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:      "json",
		EncoderConfig: newProductionEncoderConfig(),
		OutputPaths:   []string{"stdout"},
	}
}

func newDevelopmentEncoderConfig() zapcore.EncoderConfig {
	encoderConfig := newProductionEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	encoderConfig.NameKey = ""
	return encoderConfig
}

func newProductionEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "severity",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// New creates a new logger with a default "logger" field so we can identify the
// source of log messages.
func New(name string) *zap.Logger {
	return baseLogger.Named(name)
}

func GetFields(ctx context.Context) []zap.Field {
	f := ctx.Value(contextFieldsKey)
	if f == nil {
		return []zap.Field{}
	}
	return f.([]zap.Field)
}

func AddFields(ctx context.Context, fields ...zap.Field) context.Context {
	f := GetFields(ctx)
	f = append(f, fields...)
	return context.WithValue(ctx, contextFieldsKey, f)
}

func LevelHandler(w http.ResponseWriter, r *http.Request) {
	baseConfig.Level.ServeHTTP(w, r)
}
