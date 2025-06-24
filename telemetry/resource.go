package telemetry

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/replicate/go/telemetry/detectors/fly"
	"github.com/replicate/go/version"
)

var (
	defaultResource     *resource.Resource
	defaultResourceOnce sync.Once
)

const otelResourceAttributesEnvVar = "OTEL_RESOURCE_ATTRIBUTES"

// cleanOTELResourceAttributes removes empty key=value pairs and trailing commas
// from OTEL_RESOURCE_ATTRIBUTES to prevent "partial resource: missing value" errors.
// This addresses the issue where empty environment variables like COG_VERSION_OVERRIDE=""
// create malformed attributes that OpenTelemetry rejects.
func cleanOTELResourceAttributes() {
	attrs := os.Getenv(otelResourceAttributesEnvVar)
	if attrs == "" {
		return
	}

	// Split by comma and filter out empty parts
	parts := strings.Split(attrs, ",")
	var cleaned []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Check if this is a valid key=value pair
		if idx := strings.Index(part, "="); idx > 0 && idx < len(part)-1 {
			// Has both key and value
			cleaned = append(cleaned, part)
		}
		// Skip malformed entries like "key=" or just "="
	}

	// Update the environment variable with cleaned attributes
	if len(cleaned) > 0 {
		os.Setenv(otelResourceAttributesEnvVar, strings.Join(cleaned, ","))
	} else {
		// If no valid attributes remain, unset the variable
		os.Unsetenv(otelResourceAttributesEnvVar)
	}
}

func DefaultResource() *resource.Resource {
	defaultResourceOnce.Do(func() {
		// Clean OTEL_RESOURCE_ATTRIBUTES before creating resource to prevent
		// "partial resource: missing value" errors from empty values
		cleanOTELResourceAttributes()

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
