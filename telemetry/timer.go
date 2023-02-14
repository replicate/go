package telemetry

import (
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func Timer(span trace.Span) func(string) {
	mark := time.Now()

	// set an attribute with the timestamp, so we can see unexpected delays
	// before the timer is started (eg in middleware)
	span.SetAttributes(attribute.Float64("timings.timer_started_at", float64(mark.UnixMilli())/1000))

	return func(label string) {
		val := time.Since(mark).Milliseconds()
		key := fmt.Sprintf("timings.%s", label)
		span.SetAttributes(attribute.Int64(key, val))
	}
}
