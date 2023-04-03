package telemetry

import (
	"fmt"

	"github.com/getsentry/sentry-go"
)

type ErrorHandler struct{}

func (eh ErrorHandler) Handle(err error) {
	log := logger.Sugar()

	err = fmt.Errorf("opentelemetry error: %w", err)

	log.Warn(err)
	sentry.CaptureException(err)
}
