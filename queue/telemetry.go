package queue

import (
	"time"

	"go.opentelemetry.io/otel/attribute"
)

var pickupDelayKey = attribute.Key("queue.pickup_delay_ms")

func pickupDelay(d time.Duration) attribute.KeyValue {
	return pickupDelayKey.Int64(d.Milliseconds())
}
