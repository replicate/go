package telemetry

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/replicate/go/logging"
	"github.com/replicate/go/version"
)

var logger = logging.New("telemetry")

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

	otel.SetErrorHandler(ErrorHandler{})
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		&TraceOptionsPropagator{
			Next: propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{},
				propagation.Baggage{},
			),
		},
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

func TraceContextFromContext(ctx context.Context) propagation.MapCarrier {
	c := propagation.MapCarrier{}
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, c)
	return c
}

func createTracerProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	// The exporter uses the OTEL_EXPORTER_OTLP_ENDPOINT and
	// OTEL_EXPORTER_OTLP_HEADERS environment variables.
	exp, err := otlptrace.New(ctx, otlptracehttp.NewClient())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize trace exporter: %w", err)
	}

	// The default resource uses the OTEL_SERVICE_NAME environment variable.
	defaultResource := resource.Default()
	detectedResource, err := resource.New(
		ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithDetectors(gcp.NewDetector()),
		resource.WithAttributes(defaultSpanAttributes()...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to detect resource attributes: %w", err)
	}

	rsrc, err := resource.Merge(defaultResource, detectedResource)
	if err != nil {
		return nil, fmt.Errorf("failed to merge resources: %w", err)
	}

	var sp sdktrace.SpanProcessor
	sp = sdktrace.NewBatchSpanProcessor(exp)
	sp = &DroppedDataProcessor{Next: sp} // this should remain next-to-last in the chain
	sp = &TraceOptionsProcessor{Next: sp}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sp), sdktrace.WithResource(rsrc))

	return tp, nil
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
