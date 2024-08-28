package fly

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var _ resource.Detector = (*detector)(nil)

type detector struct{}

func NewDetector() resource.Detector {
	return &detector{}
}

func (d *detector) Detect(_ context.Context) (*resource.Resource, error) {
	if !isFly() {
		return resource.Empty(), nil
	}

	attrs := []attribute.KeyValue{
		semconv.CloudProviderKey.String("fly"),
	}

	attrs = addEnvAttr(attrs, "FLY_APP_NAME", attribute.Key("fly.app_name"))
	attrs = addEnvAttr(attrs, "FLY_IMAGE_REF", attribute.Key("fly.image_ref"))
	attrs = addEnvAttr(attrs, "FLY_MACHINE_ID",
		semconv.ServiceInstanceIDKey,
		attribute.Key("fly.machine_id"),
	)
	attrs = addEnvAttr(attrs, "FLY_MACHINE_VERSION", attribute.Key("fly.machine_version"))
	attrs = addEnvAttr(attrs, "FLY_PUBLIC_IP", attribute.Key("fly.public_ip"))
	attrs = addEnvAttr(attrs, "FLY_PRIVATE_IP", attribute.Key("fly.private_ip"))
	attrs = addEnvAttr(attrs, "FLY_PROCESS_GROUP", attribute.Key("fly.process_group"))
	attrs = addEnvAttr(attrs, "FLY_REGION",
		semconv.CloudRegionKey,
		attribute.Key("fly.region"),
	)

	return resource.NewWithAttributes(semconv.SchemaURL, attrs...), nil
}

func isFly() bool {
	_, ok := os.LookupEnv("FLY_APP_NAME")
	return ok
}

func addEnvAttr(attrs []attribute.KeyValue, name string, keys ...attribute.Key) []attribute.KeyValue {
	if value, ok := os.LookupEnv(name); ok {
		for _, key := range keys {
			attrs = append(attrs, key.String(value))
		}
	}
	return attrs
}
