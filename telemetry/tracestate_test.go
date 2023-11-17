package telemetry

import (
	"context"
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

func TestTraceOptionsUsesSpanContextTraceOptions(t *testing.T) {
	ctx := context.Background()

	ts := trace.TraceState{}
	ts, _ = ts.Insert("r8/dl", "full")
	ts, _ = ts.Insert("r8/sm", "always")

	scc := makeValidSpanContextConfig()
	scc.TraceState = ts
	sc := trace.NewSpanContext(scc)
	ctx = trace.ContextWithSpanContext(ctx, sc)

	to := TraceOptionsFromContext(ctx)
	assert.Equal(t, DetailLevelFull, to.DetailLevel)
	assert.Equal(t, SampleModeAlways, to.SampleMode)
}
