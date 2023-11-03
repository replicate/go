package semconv

import "go.opentelemetry.io/otel/attribute"

// DisableSampling disables tail sampling when set on any span of the trace.
// This works because Refinery is configured to set SampleRate to 1 when it sees
// this attribute.
var DisableSampling = attribute.Bool("meta.replicate.disable_sampling", true)
