package errors

import (
	"context"
	"net/http"
	"os"

	"github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"

	"github.com/replicate/go/logging"
	"github.com/replicate/go/version"
)

var logger = logging.New("errors")

func Init() {
	log := logger.Sugar()

	sentryDSN := os.Getenv("SENTRY_DSN")
	if sentryDSN == "" {
		log.Warn("SENTRY_DSN not set: skipping Sentry initialization!")
		return
	}

	logger.Info("Initializing Sentry")
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              sentryDSN,
		AttachStacktrace: true,
		Release:          version.Version(),
	})
	if err != nil {
		log.Warnw("Failed to initialize Sentry client", "error", err)
	}
}

// GetHub returns the current Sentry Hub from the context, or fetches the
// default hub and attaches it to the context if needed.
func GetHub(ctx context.Context) (context.Context, *sentry.Hub) {
	if hub := sentry.GetHubFromContext(ctx); hub != nil {
		return ctx, hub
	}

	// Under normal circumstances if we're calling this there should be a Hub on
	// the passed context. But in test code the middleware might not have been set
	// up, and so it's better to guarantee that we return a non-nil Hub.
	hub := sentry.CurrentHub()
	ctx = sentry.SetHubOnContext(ctx, hub)
	return ctx, hub
}

func SetHub(ctx context.Context, hub *sentry.Hub) context.Context {
	return sentry.SetHubOnContext(ctx, hub)
}

func Middleware() func(http.Handler) http.Handler {
	handler := sentryhttp.New(sentryhttp.Options{
		Repanic: true,
	})
	return handler.Handle
}
