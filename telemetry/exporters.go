package telemetry

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/getsentry/sentry-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
)

// check that UTF8ErrorCatchingExporter implements trace.SpanExporter
var _ trace.SpanExporter = new(UTF8ErrorCatchingExporter)

type UTF8ErrorCatchingExporter struct {
	next trace.SpanExporter
}

type CapturedAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Type  string `json:"type"`
}
type CapturedSpanData struct {
	SpanName   string               `json:"span"`
	TraceID    string               `json:"trace_id"`
	SpanID     string               `json:"span_id"`
	SpanKind   string               `json:"span_kind"`
	Attributes []attribute.KeyValue `json:"attributes"`
}

func (u *UTF8ErrorCatchingExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	log := logger.Sugar()
	err := u.next.ExportSpans(ctx, spans)
	if err != nil && strings.Contains(err.Error(), "invalid UTF-8") {
		spanData := make([]CapturedSpanData, 0)
		for _, span := range spans {
			spanData = append(spanData, CapturedSpanData{
				SpanName:   span.Name(),
				TraceID:    span.SpanContext().TraceID().String(),
				SpanID:     span.SpanContext().SpanID().String(),
				SpanKind:   span.SpanKind().String(),
				Attributes: span.Attributes(),
			})
		}
		// log all span data
		log.Errorw("Span data", "span", spanData)
		spanJSON, marshalErr := json.Marshal(spanData)
		if marshalErr != nil {
			log.Errorw("Error marshalling span data", "error", marshalErr)
			sentry.CaptureException(marshalErr)
			return marshalErr
		}
		// Send to Sentry
		sentry.CaptureMessage(string(spanJSON))
	}
	return err
}

func (u *UTF8ErrorCatchingExporter) Shutdown(ctx context.Context) error {
	return u.next.Shutdown(ctx)
}
