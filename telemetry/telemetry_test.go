package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStart is the most basic of smoke tests to ensure that we can at least
// instantiate and start the telemetry package.
func TestStart(t *testing.T) {
	ctx := context.Background()

	tel, err := Start(ctx)

	require.NoError(t, err)

	require.NoError(t, tel.Shutdown(ctx))
}

func TestTraceContextFromContext(t *testing.T) {
	ctx := context.Background()

	tel, err := Start(ctx)
	require.NoError(t, err)

	ctx, span := Tracer("test", "trace_context_test").Start(ctx, "my-span")
	defer span.End()

	carrier := TraceContextFromContext(ctx)

	// The carrier should be a map-like object containing at least traceparent
	// information. (It may also contain baggage.)
	assert.Contains(t, carrier, "traceparent")

	require.NoError(t, tel.Shutdown(ctx))
}
