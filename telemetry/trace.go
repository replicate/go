package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/replicate/go/version"
)

// Tracer fetches a tracer, applying a standard naming convention for use across
// services.
func Tracer(service string, component string, opts ...trace.TracerOption) trace.Tracer {
	name := fmt.Sprintf("replicate/%s/%s", service, component)
	opts = append(opts, trace.WithInstrumentationVersion(version.Version()))
	return otel.Tracer(name, opts...)
}

// TraceContextFromContext returns the tracecontext present in the passed
// context, if any.
func TraceContextFromContext(ctx context.Context) propagation.MapCarrier {
	c := propagation.MapCarrier{}
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, c)
	return c
}

// WithTraceContext adds the tracecontext from the provided carrier to a
// returned Context. If no valid tracecontext is contained in the carrier, the
// passed ctx will be returned directly.
func WithTraceContext(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, carrier)
}

func configureTracerProvider() {
	tp, err := CreateTracerProvider(context.Background())
	if err != nil {
		logger.Warn("failed to create tracer provider", zap.Error(err))
		return
	}

	otel.SetTracerProvider(tp)
}

func CreateTracerProvider(ctx context.Context, opts ...sdktrace.TracerProviderOption) (*sdktrace.TracerProvider, error) {
	exp, err := otlptrace.New(ctx, otlptracehttp.NewClient())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize trace exporter: %w", err)
	}

	wrappedExp := &UTF8ErrorCatchingExporter{next: exp}

	var sp sdktrace.SpanProcessor
	sp = sdktrace.NewBatchSpanProcessor(wrappedExp)
	sp = &DroppedDataProcessor{Next: sp} // this should remain next-to-last in the chain
	sp = &TraceOptionsProcessor{Next: sp}

	opts = append(
		opts,
		sdktrace.WithSpanProcessor(sp),
		sdktrace.WithResource(DefaultResource()),
	)
	tp := sdktrace.NewTracerProvider(opts...)
	return tp, nil
}
