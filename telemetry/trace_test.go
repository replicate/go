package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestInit is the most basic of smoke tests to ensure that we can at least
// instantiate and start the telemetry package.
func TestInit(t *testing.T) {
	// This is usually called by package init, but here we call it explicitly so
	// the lack of an OTEL_EXPORTER_OTLP_ENDPOINT doesn't cause us to skip it.
	configureTracerProvider()
	configureMeterProvider(false)

	tp := otel.GetTracerProvider()
	assert.IsType(t, &sdktrace.TracerProvider{}, tp)

	ctx := context.Background()
	require.NoError(t, Shutdown(ctx))
}

func TestTraceContextFromContext(t *testing.T) {
	// This is usually called by package init, but here we call it explicitly so
	// the lack of an OTEL_EXPORTER_OTLP_ENDPOINT doesn't cause us to skip it.
	configureTracerProvider()
	configureMeterProvider(false)

	ctx := context.Background()
	ctx, span := Tracer("test", "trace_context_test").Start(ctx, "my-span")
	defer span.End()

	carrier := TraceContextFromContext(ctx)

	// The carrier should be a map-like object containing at least traceparent
	// information. (It may also contain baggage.)
	assert.Contains(t, carrier, "traceparent")

	require.NoError(t, Shutdown(ctx))
}
