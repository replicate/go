package flags

import (
	"fmt"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"go.opentelemetry.io/otel/attribute"
)

// SpanAttributes returns a slice of span attributes containing the values of
// all feature flags (under the "flags." namespace).
func SpanAttributes(user *ldcontext.Context) (attrs []attribute.KeyValue) {
	if currentClient == nil {
		return
	}

	allFlags := currentClient.AllFlagsState(*user)

	for key, value := range allFlags.ToValuesMap() {
		spanKey := fmt.Sprintf("flags.%s", key)
		switch {
		case value.IsBool():
			attrs = append(attrs, attribute.Bool(spanKey, value.BoolValue()))
		case value.IsNull():
			// Do nothing!
		default:
			// For all other value types, use a string representation
			attrs = append(attrs, attribute.String(spanKey, value.String()))
		}
	}

	return
}
