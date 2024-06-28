package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"

	"github.com/replicate/go/logging"
	"github.com/replicate/go/version"
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

func defaultSpanAttributes() []attribute.KeyValue {
	attrs := []attribute.KeyValue{}

	attrs = append(attrs, semconv.ServiceVersionKey.String(version.Version()))

	if hostname, err := os.Hostname(); err == nil {
		attrs = append(attrs, semconv.ServiceInstanceIDKey.String(hostname))
	}

	// Detect when running on Fly.
	//
	// TODO: it might be nice to turn this into a resource.Detector at some point.
	attrs = tryAddEnvAttribute(attrs, "FLY_ALLOC_ID", "fly.alloc_id")
	attrs = tryAddEnvAttribute(attrs, "FLY_APP_NAME", "fly.app_name")
	attrs = tryAddEnvAttribute(attrs, "FLY_IMAGE_REF", "fly.image_ref")
	attrs = tryAddEnvAttribute(attrs, "FLY_PUBLIC_IP", "fly.public_ip")
	attrs = tryAddEnvAttribute(attrs, "FLY_REGION", "fly.region")

	return attrs
}

func tryAddEnvAttribute(attrs []attribute.KeyValue, envName string, key string) []attribute.KeyValue {
	if value, ok := os.LookupEnv(envName); ok {
		attrs = append(attrs, attribute.String(key, value))
	}
	return attrs
}
