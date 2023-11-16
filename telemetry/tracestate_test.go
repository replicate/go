package telemetry

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceOptionsFromContextDefaults(t *testing.T) {
	ctx := context.Background()

	// By default, trace options should have their default values
	to := TraceOptionsFromContext(ctx)
	assert.Equal(t, DetailLevelDefault, to.DetailLevel)
	assert.Equal(t, SampleModeDefault, to.SampleMode)
}

func TestWithTraceOptionsSetsTraceOptions(t *testing.T) {
	ctx := context.Background()

	// Setting new TraceOptions should work as expected
	ctx = WithTraceOptions(ctx, TraceOptions{
		DetailLevel: DetailLevelFull,
		SampleMode:  SampleModeAlways,
	})

	to := TraceOptionsFromContext(ctx)
	assert.Equal(t, DetailLevelFull, to.DetailLevel)
	assert.Equal(t, SampleModeAlways, to.SampleMode)
}

func TestWithTraceOptionsSerialization(t *testing.T) {
	ctx := context.Background()

	ctx = WithTraceOptions(ctx, TraceOptions{
		DetailLevel: DetailLevelFull,
		SampleMode:  SampleModeAlways,
	})

	tsString := trace.SpanContextFromContext(ctx).TraceState().String()
	elements := strings.Split(tsString, ",")

	assert.Contains(t, elements, "r8/sm=always")
	assert.Contains(t, elements, "r8/dl=full")
}
