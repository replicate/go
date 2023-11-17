package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func makeValidSpanContextConfig() trace.SpanContextConfig {
	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	return trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}
}

func makeValidSpanContext() trace.SpanContext {
	return trace.NewSpanContext(makeValidSpanContextConfig())
}

// Check that we're correctly passing the work onto the Next propagator.
func TestTraceOptionsPropagatorUsesNextPropagator(t *testing.T) {
	ctx := context.Background()
	ctx = trace.ContextWithSpanContext(ctx, makeValidSpanContext())
	propagator := TraceOptionsPropagator{
		Next: propagation.TraceContext{},
	}
	carrier := propagation.MapCarrier{}

	propagator.Inject(ctx, carrier)

	require.Contains(t, carrier, "traceparent")
}

// Check that TraceOptions are respected both in SpanContext and
// (preferentially) from the Context itself.
func TestTraceOptionsPropagatorInjectsTraceOptions(t *testing.T) {
	ctx := context.Background()

	ts := trace.TraceState{}
	ts, _ = ts.Insert("r8/sm", "always")

	scc := makeValidSpanContextConfig()
	scc.TraceState = ts
	ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(scc))
	propagator := TraceOptionsPropagator{
		Next: propagation.TraceContext{},
	}

	// First check that only the sample mode field is set
	{
		carrier := propagation.MapCarrier{}
		propagator.Inject(ctx, carrier)
		require.Contains(t, carrier, "tracestate")
		assert.Equal(t, carrier["tracestate"], "r8/sm=always")
	}

	// Then update TraceOptions locally and ensure that the values override those
	// set in the SpanContext.
	{
		ctx := WithTraceOptions(ctx, TraceOptions{
			DetailLevel: DetailLevelFull,
			SampleMode:  SampleModeNever,
		})
		carrier := propagation.MapCarrier{}
		propagator.Inject(ctx, carrier)
		require.Contains(t, carrier, "tracestate")
		assert.Contains(t, carrier["tracestate"], "r8/sm=never")
		assert.Contains(t, carrier["tracestate"], "r8/dl=full")
	}
}

func TestTraceOptionsPropagatorPrefersTraceOptionsFromContext(t *testing.T) {
	ctx := trace.ContextWithSpanContext(context.Background(), makeValidSpanContext())
	ctx = WithTraceOptions(ctx, TraceOptions{
		DetailLevel: DetailLevelFull,
		SampleMode:  SampleModeAlways,
	})

	propagator := TraceOptionsPropagator{
		Next: propagation.TraceContext{},
	}
	carrier := propagation.MapCarrier{}

	propagator.Inject(ctx, carrier)

	require.Contains(t, carrier, "tracestate")
	assert.Contains(t, carrier["tracestate"], "r8/sm=always")
	assert.Contains(t, carrier["tracestate"], "r8/dl=full")
}
