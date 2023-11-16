package telemetry

import (
	"context"

	"github.com/replicate/go/telemetry/semconv"
	"go.opentelemetry.io/otel/sdk/trace"
)

// Check TraceOptionsProcessor implements SpanProcessor
var _ trace.SpanProcessor = new(TraceOptionsProcessor)

// TraceOptionsProcessor handles any custom span handling related to our own
// TraceOptions. At the moment it takes care of adding attributes to the span
// that control sampling and detail level.
type TraceOptionsProcessor struct {
	Next trace.SpanProcessor
}

func (p *TraceOptionsProcessor) OnStart(parent context.Context, s trace.ReadWriteSpan) {
	to := TraceOptionsFromContext(parent)

	if to.SampleMode == SampleModeAlways {
		s.SetAttributes(semconv.DisableSampling)
	}

	// TODO: record all of the trace options on the spans?

	p.Next.OnStart(parent, s)
}

func (p *TraceOptionsProcessor) OnEnd(s trace.ReadOnlySpan) {
	p.Next.OnEnd(s)
}

func (p *TraceOptionsProcessor) Shutdown(ctx context.Context) error {
	return p.Next.Shutdown(ctx)
}

func (p *TraceOptionsProcessor) ForceFlush(ctx context.Context) error {
	return p.Next.ForceFlush(ctx)
}
