package telemetry

import (
	"context"
	"errors"
	"strings"

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
	switch {
	case logOnlyError(err):
		// do not capture these errors in sentry
	default:
		sentry.CaptureException(err)
	}
}

func logOnlyError(err error) bool {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return true
	// Server said go away
	case strings.Contains(err.Error(), "GO AWAY"):
		return true
	// Connection reset by peer, this isn't something we can do much about
	case strings.Contains(err.Error(), "connection reset by peer"):
		return true
	// processor export timeout, this is not actionable by us, happens in `POST`
	case strings.Contains(err.Error(), "processor export timeout"):
		return true
	// cannot rewind body, happens when retrying a request with a non-rewindable body
	case strings.Contains(err.Error(), "cannot rewind body"):
		return true
	}
	return false
}
