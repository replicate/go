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
	configureMeterProvider(false)

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		logger.Warn("traces will not be exported via OTLP (OTEL_EXPORTER_OTLP_ENDPOINT is not set)")
		return
	}

	configureTracerProvider()
}

func Shutdown(ctx context.Context) error {
	if tp, ok := otel.GetTracerProvider().(*trace.TracerProvider); ok && tp != nil {
		if err := tp.Shutdown(ctx); err != nil {
			return err
		}
	}
	if mp, ok := otel.GetMeterProvider().(*metric.MeterProvider); ok && mp != nil {
		if err := mp.Shutdown(ctx); err != nil {
			return err
		}
	}
	return nil
}
