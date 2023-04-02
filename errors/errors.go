package errors

import (
	"net/http"
	"os"

	"github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"

	"github.com/replicate/go/logging"
	"github.com/replicate/go/version"
)

var logger = logging.New("errors")

func Init() {
	sentryDSN := os.Getenv("SENTRY_DSN")
	if sentryDSN == "" {
		logger.Warn("SENTRY_DSN not set: skipping Sentry initialization!")
		return
	}

	logger.Info("Initializing Sentry")
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              sentryDSN,
		AttachStacktrace: true,
		Release:          version.Version(),
	})
	if err != nil {
		logger.Warnf("Failed to initialize Sentry client: %v", err)
	}
}

func Middleware() func(http.Handler) http.Handler {
	handler := sentryhttp.New(sentryhttp.Options{
		Repanic: true,
	})
	return handler.Handle
}
