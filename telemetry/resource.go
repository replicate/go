package telemetry

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"

	"github.com/replicate/go/telemetry/detectors/fly"
	"github.com/replicate/go/version"
)

var (
	defaultResource     *resource.Resource
	defaultResourceOnce sync.Once
)

func DefaultResource() *resource.Resource {
	defaultResourceOnce.Do(func() {
		var err error
		defaultResource, err = resource.New(
			context.Background(),
			resource.WithSchemaURL(semconv.SchemaURL),
			resource.WithFromEnv(),
			resource.WithTelemetrySDK(),
			resource.WithHost(),
			resource.WithDetectors(
				// We'd love to use the AWS EKS resource detector here too, but it's
				// mostly useless: https://github.com/open-telemetry/opentelemetry-go-contrib/issues/1856
				gcp.NewDetector(),
				fly.NewDetector(),
			),
			resource.WithAttributes(semconv.ServiceVersion(version.Version())),
		)
		switch {
		case errors.Is(err, resource.ErrPartialResource):
			// ignored
		case err != nil:
			otel.Handle(err)
		}
		if defaultResource == nil {
			defaultResource = resource.Empty()
		}
	})

	return defaultResource
}
