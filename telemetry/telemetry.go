package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/replicate/go/logging"
)

var logger = logging.New("telemetry")

func init() {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		logger.Warn("using no-op tracer provider (OTEL_EXPORTER_OTLP_ENDPOINT is not set)")
		return
	}

	configureTracerProvider()
}

func Shutdown(ctx context.Context) error {
	if tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
		return tp.Shutdown(ctx)
	}
	return nil
}
