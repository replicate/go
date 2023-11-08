package telemetry

// TraceOptions represents additional configuration for a trace that can be
// propagated throughout the system.
//
// In future it might make sense to reimplement this as Baggage that can be
// transported directly in TraceContext
type TraceOptions struct {
	// Whether sampling should be disabled for spans associated with this
	// activity.
	DisableSampling bool `json:"disable_sampling,omitempty"`

	// Whether to collect a full trace for this activity.
	//
	// In a full trace we record all activities under the same trace ID. This can
	// be helpful for debugging purposes. FullTrace is usually only helpful if
	// DisableSampling is set at the same time.
	FullTrace bool `json:"full_trace,omitempty"`
}
