package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	TraceStateKeyDetailLevel = "r8/dl"
	TraceStateKeySampleMode  = "r8/sm"
)

const (
	DetailLevelDefault DetailLevel = iota
	DetailLevelFull
)

const (
	SampleModeNever SampleMode = iota - 1
	SampleModeDefault
	SampleModeAlways
)

// Don't forget to rerun `go generate ./...` if you add or remove values from
// the DetailLevel or SampleMode enums.
//
//go:generate stringer -type=DetailLevel,SampleMode -output=tracestate_string.go
type DetailLevel int
type SampleMode int

var (
	detailLevels = map[string]DetailLevel{
		"":     DetailLevelDefault,
		"full": DetailLevelFull,
	}
	sampleModes = map[string]SampleMode{
		"":       SampleModeDefault,
		"always": SampleModeAlways,
		"never":  SampleModeNever,
	}
)

type TraceOptions struct {
	// How much detail to gather for this trace. DetailLevelFull enables
	// additional spans and trace context propagation which can enable a full
	// end-to-end trace, even for activities that are usually gathered under
	// separate trace IDs.
	DetailLevel DetailLevel
	// How to sample this trace. Controls the addition of attributes to trace
	// spans which serve as hints to our tail sampling proxy (Refinery) on how to
	// sample the trace.
	SampleMode SampleMode
}

// TraceOptionsFromContext extracts any custom trace options from the trace
// state carried in the passed context.
func TraceOptionsFromContext(ctx context.Context) TraceOptions {
	ts := trace.SpanContextFromContext(ctx).TraceState()
	return parseTraceOptions(ts)
}

// WithTraceOptions returns a copy of the provided context with the passed
// TraceOptions set.
func WithTraceOptions(ctx context.Context, to TraceOptions) context.Context {
	sc := trace.SpanContextFromContext(ctx)
	ts := setTraceOptions(sc.TraceState(), to)
	return trace.ContextWithSpanContext(ctx, sc.WithTraceState(ts))
}

// WithFullTrace returns a new context with full tracing mode enabled. This
// sets the trace detail level to "full" and the sample mode to "always".
func WithFullTrace(ctx context.Context) context.Context {
	to := TraceOptionsFromContext(ctx)
	to.DetailLevel = DetailLevelFull
	to.SampleMode = SampleModeAlways
	return WithTraceOptions(ctx, to)
}

func parseTraceOptions(ts trace.TraceState) TraceOptions {
	to := TraceOptions{}

	if d, ok := detailLevels[ts.Get(TraceStateKeyDetailLevel)]; ok {
		to.DetailLevel = d
	}
	if s, ok := sampleModes[ts.Get(TraceStateKeySampleMode)]; ok {
		to.SampleMode = s
	}

	return to
}

func setTraceOptions(ts trace.TraceState, to TraceOptions) trace.TraceState {
	var tsOut = ts

	if to.DetailLevel == DetailLevelDefault {
		tsOut = tsOut.Delete(TraceStateKeyDetailLevel)
	} else {
		if value, ok := invertMap(detailLevels)[to.DetailLevel]; ok {
			ts, err := tsOut.Insert(TraceStateKeyDetailLevel, value)
			if err != nil {
				logger.Warn("error adding tracestate", zap.Error(err))
			}
			tsOut = ts
		}
	}

	if to.SampleMode == SampleModeDefault {
		tsOut = tsOut.Delete(TraceStateKeySampleMode)
	} else {
		if value, ok := invertMap(sampleModes)[to.SampleMode]; ok {
			ts, err := tsOut.Insert(TraceStateKeySampleMode, value)
			if err != nil {
				logger.Warn("error adding tracestate", zap.Error(err))
			}
			tsOut = ts
		}
	}

	return tsOut
}

func invertMap[T comparable, U comparable](in map[T]U) map[U]T {
	out := make(map[U]T, len(in))
	for t, u := range in {
		out[u] = t
	}
	return out
}
