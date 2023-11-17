package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Check TraceOptionsPropagator implements TextMapPropagator
var _ propagation.TextMapPropagator = new(TraceOptionsPropagator)

type TraceOptionsPropagator struct {
	Next propagation.TextMapPropagator
}

func (p *TraceOptionsPropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return
	}

	// If TraceOptions has been set directly in the context, then replace the
	// SpanContext with one that has the appropriate TraceState.
	//
	// Note: it is generally only safe to do this in a propagator or an exporter.
	if to, ok := traceOptionsFromContextOnly(ctx); ok {
		ts := setTraceOptions(sc.TraceState(), to)
		ctx = trace.ContextWithSpanContext(ctx, sc.WithTraceState(ts))
	}

	p.Next.Inject(ctx, carrier)
}

func (p *TraceOptionsPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return p.Next.Extract(ctx, carrier)
}

func (p *TraceOptionsPropagator) Fields() []string {
	return p.Next.Fields()
}
