package telemetry

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/replicate/go/version"
)

type Telemetry struct {
	*sdktrace.TracerProvider
}

// Start configures the global tracer provider and returns a handle to it so it
// can be shut down.
func Start(ctx context.Context) (*Telemetry, error) {
	tp, err := createTracerProvider(ctx)
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	return &Telemetry{tp}, nil
}

// Tracer fetches a tracer, applying a standard naming convention for use across
// services.
func Tracer(service string, component string, opts ...trace.TracerOption) trace.Tracer {
	name := fmt.Sprintf("replicate/%s/%s", service, component)
	opts = append(opts, trace.WithInstrumentationVersion(version.Version()))
	return otel.Tracer(name, opts...)
}

func createTracerProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	// The exporter uses the OTEL_EXPORTER_OTLP_ENDPOINT and
	// OTEL_EXPORTER_OTLP_HEADERS environment variables.
	exp, err := otlptrace.New(ctx, otlptracehttp.NewClient())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize trace exporter: %w", err)
	}

	// The default resource uses the OTEL_SERVICE_NAME environment variable.
	rsrc, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, defaultSpanAttributes()...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource for tracer provider: %w", err)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(rsrc))

	return tp, nil
}

func defaultSpanAttributes() []attribute.KeyValue {
	hostName := os.Getenv("HOSTNAME")

	return []attribute.KeyValue{
		semconv.K8SPodNameKey.String(hostName),
		semconv.ServiceInstanceIDKey.String(hostName),
		semconv.ServiceVersionKey.String(version.Version()),
	}
}
