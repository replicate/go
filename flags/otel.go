package flags

import (
	"fmt"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// SetSpanAttributes will record the values of all feature flags on the passed
// span under the "flags." namespace.
func SetSpanAttributes(user *ldcontext.Context, span trace.Span) {
	if currentClient == nil {
		return
	}

	allFlags := currentClient.AllFlagsState(*user)

	for key, value := range allFlags.ToValuesMap() {
		spanKey := fmt.Sprintf("flags.%s", key)
		switch {
		case value.IsBool():
			span.SetAttributes(attribute.Bool(spanKey, value.BoolValue()))
		case value.IsNull():
			// Do nothing!
		default:
			// For all other value types, use a string representation
			span.SetAttributes(attribute.String(spanKey, value.String()))
		}
	}
}
