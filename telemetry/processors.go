package telemetry

import (
	"context"

	"github.com/replicate/go/telemetry/semconv"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// Check DroppedDataProcessor implements SpanProcessor
var _ trace.SpanProcessor = new(DroppedDataProcessor)

// DroppedDataProcessor logs warnings when spans are dropping data due to
// attribute/event/link count limits.
//
// See the various environment variables that control span limits at
// https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#span-limits
type DroppedDataProcessor struct {
	Next trace.SpanProcessor
}

func (p *DroppedDataProcessor) OnStart(parent context.Context, s trace.ReadWriteSpan) {
	p.Next.OnStart(parent, s)
}

func (p *DroppedDataProcessor) OnEnd(s trace.ReadOnlySpan) {
	if s.DroppedAttributes() > 0 || s.DroppedLinks() > 0 || s.DroppedEvents() > 0 {
		sc := s.SpanContext()
		// TODO: we might in future want to throttle the rate at which we emit this
		// warning, as this could get very chatty indeed.
		logger.Warn(
			"span data dropped due to limits",
			zap.Int("dropped_attributes", s.DroppedAttributes()),
			zap.Int("dropped_links", s.DroppedLinks()),
			zap.Int("dropped_events", s.DroppedEvents()),
			zap.String("trace_id", sc.TraceID().String()),
			zap.String("span_id", sc.SpanID().String()),
		)
	}
	p.Next.OnEnd(s)
}

func (p *DroppedDataProcessor) Shutdown(ctx context.Context) error {
	return p.Next.Shutdown(ctx)
}

func (p *DroppedDataProcessor) ForceFlush(ctx context.Context) error {
	return p.Next.ForceFlush(ctx)
}

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
