package telemetry

import (
	"github.com/getsentry/sentry-go"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func init() {
	otel.SetErrorHandler(ErrorHandler{})
}

type ErrorHandler struct{}

func (eh ErrorHandler) Handle(err error) {
	// +1 for this wrapper, +3 for opentelemetry-go's internal error handling code
	log := logger.WithOptions(zap.AddCallerSkip(4))
	log.Warn("opentelemetry error", zap.Error(err))
	sentry.CaptureException(err)
}
