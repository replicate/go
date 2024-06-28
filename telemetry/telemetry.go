package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"

	"github.com/replicate/go/logging"
)

var logger = logging.New("telemetry")

func init() {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		logger.Warn("metrics/traces will not be exported via OTLP (OTEL_EXPORTER_OTLP_ENDPOINT is not set)")
		configureMeterProvider(false)
		return
	}

	configureMeterProvider(true)
	configureTracerProvider()
}

func Shutdown(ctx context.Context) error {
	if tp, ok := otel.GetTracerProvider().(*trace.TracerProvider); ok {
		return tp.Shutdown(ctx)
	}
	if mp, ok := otel.GetMeterProvider().(*metric.MeterProvider); ok {
		return mp.Shutdown(ctx)
	}
	return nil
}
