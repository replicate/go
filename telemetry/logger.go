package telemetry

import (
	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	log := logr.New(&zapAdapter{logger: logger})
	otel.SetLogger(log)
}

type zapAdapter struct {
	logger *zap.Logger
}

func (z *zapAdapter) Init(info logr.RuntimeInfo) {
	// +1 for this wrapper
	// +1 for opentelemetry-go's internal_logging.go
	skip := info.CallDepth + 2
	z.logger = z.logger.WithOptions(zap.AddCallerSkip(skip))
}

func (z *zapAdapter) Enabled(level int) bool {
	return z.logger.Core().Enabled(mapLevel(level))
}

func (z *zapAdapter) Info(level int, msg string, keysAndValues ...any) {
	z.logger.Sugar().Logw(mapLevel(level), msg, keysAndValues...)
}

func (z *zapAdapter) Error(err error, msg string, keysAndValues ...any) {
	keysAndValues = append(keysAndValues, "error", err)
	z.logger.Sugar().Errorw(msg, keysAndValues...)
}

func (z *zapAdapter) WithValues(keysAndValues ...any) logr.LogSink {
	return &zapAdapter{
		logger: z.logger.Sugar().With(keysAndValues...).Desugar(),
	}
}

func (z *zapAdapter) WithName(name string) logr.LogSink {
	return &zapAdapter{
		logger: z.logger.Named(name),
	}
}

// mapLevel maps the levels used by opentelemetry-go to zap levels.
//
// See:
//
//	https://github.com/open-telemetry/opentelemetry-go/blob/1297d5f0/internal/global/internal_logging.go#L30-L32
func mapLevel(level int) zapcore.Level {
	switch {
	case level <= 1:
		return zapcore.WarnLevel
	case level <= 4:
		return zapcore.InfoLevel
	default:
		return zapcore.DebugLevel
	}
}
