package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/replicate/go/version"
)

// We serve metrics for Prometheus over HTTP. If we appear to be running inside
// Kubernetes, bind to the wildcard address. Otherwise, bind to localhost.
//
// While it has some security benefits, this is mainly to prevent macOS from
// constantly badgering the user in the development environment.
var Addr = func() string {
	if _, ok := os.LookupEnv("KUBERNETES_SERVICE_HOST"); ok {
		return ":9090"
	}
	return "localhost:9090"
}()

// Meter fetches a meter, applying a standard naming convention for use across
// services.
func Meter(service string, component string, opts ...metric.MeterOption) metric.Meter {
	name := fmt.Sprintf("replicate/%s/%s", service, component)
	opts = append(opts, metric.WithInstrumentationVersion(version.Version()))
	return otel.Meter(name, opts...)
}

func configureMeterProvider(enableOTLP bool) {
	mp, err := createMeterProvider(context.Background(), enableOTLP)
	if err != nil {
		logger.Warn("failed to create meter provider", zap.Error(err))
		return
	}

	go serveMetrics()

	otel.SetMeterProvider(mp)
}

func createMeterProvider(ctx context.Context, enableOTLP bool) (metric.MeterProvider, error) {
	opts := []sdkmetric.Option{
		sdkmetric.WithResource(DefaultResource()),
	}

	// Always export metrics to Prometheus.
	prom, err := prometheus.New()
	if err != nil {
		return nil, err
	}
	opts = append(opts, sdkmetric.WithReader(prom))

	// If enabled, export metrics to OTLP as well, every 10s.
	if enableOTLP {
		exp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return nil, err
		}
		reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(10*time.Second))
		opts = append(opts, sdkmetric.WithReader(reader))
	}

	mp := sdkmetric.NewMeterProvider(opts...)
	return mp, nil
}

func serveMetrics() {
	mux := http.ServeMux{}
	mux.Handle("/metrics", promhttp.Handler())

	s := &http.Server{
		Addr:    Addr,
		Handler: &mux,
	}

	logger.Sugar().Infof("metrics server listening on %s", Addr)
	if err := s.ListenAndServe(); err != http.ErrServerClosed {
		logger.Sugar().Errorw("metrics server exited with error", "error", err)
	}
}
