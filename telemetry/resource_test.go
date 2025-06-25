package telemetry

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCleanOTELResourceAttributes tests the fix for "Shutdown error: partial resource: missing value: []"
//
// This error occurs when OTEL_RESOURCE_ATTRIBUTES contains empty values like:
// - model_container.cog_version_override= (empty value after =)
// - Trailing commas from empty EXTRA_OTEL_RESOURCE_ATTRIBUTES
func TestCleanOTELResourceAttributes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "empty values from COG_VERSION_OVERRIDE",
			// This is the actual problematic string from affected pods
			input:    "compute_unit=gpu,compute_unit_count=1,deployable.key=dp-38f7b282eeb0429f8ec38901d1899ad0,deployment.key=dp-38f7b282eeb0429f8ec38901d1899ad0,docker_image_uri=some-image:latest,hardware=nvidia-a100,k8s.container.name=director,k8s.namespace.name=models,k8s.node.name=gpu-node-1,k8s.pod.name=model-dp-38f7b282eeb0429f8ec38901d1899ad0-7d8c544fd9-k55v5,model.full_name=test-user%2Ftest-model,model.id=test-user%2Ftest-model:dp-38f,model.name=test-model,model.owner=test-user,model_container.cog_version=0.11.3,model_container.cog_version_override=,model_container.cog_version_override_raw=%3E=0.11.3,version.id=abc123,",
			expected: "compute_unit=gpu,compute_unit_count=1,deployable.key=dp-38f7b282eeb0429f8ec38901d1899ad0,deployment.key=dp-38f7b282eeb0429f8ec38901d1899ad0,docker_image_uri=some-image:latest,hardware=nvidia-a100,k8s.container.name=director,k8s.namespace.name=models,k8s.node.name=gpu-node-1,k8s.pod.name=model-dp-38f7b282eeb0429f8ec38901d1899ad0-7d8c544fd9-k55v5,model.full_name=test-user%2Ftest-model,model.id=test-user%2Ftest-model:dp-38f,model.name=test-model,model.owner=test-user,model_container.cog_version=0.11.3,model_container.cog_version_override_raw=%3E=0.11.3,version.id=abc123",
		},
		{
			name:     "trailing comma only",
			input:    "key1=value1,key2=value2,",
			expected: "key1=value1,key2=value2",
		},
		{
			name:     "multiple empty values",
			input:    "key1=value1,empty1=,key2=value2,empty2=,key3=value3",
			expected: "key1=value1,key2=value2,key3=value3",
		},
		{
			name:     "empty string between commas",
			input:    "key1=value1,,key2=value2",
			expected: "key1=value1,key2=value2",
		},
		{
			name:     "already clean attributes",
			input:    "service.name=test,service.version=1.0.0",
			expected: "service.name=test,service.version=1.0.0",
		},
		{
			name:     "empty input",
			input:    "",
			expected: "",
		},
		{
			name:     "only empty values",
			input:    "empty1=,empty2=,",
			expected: "",
		},
		{
			name:     "spaces around values",
			input:    "key1=value1, key2=value2 , empty= , key3=value3",
			expected: "key1=value1,key2=value2,key3=value3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the test input
			t.Setenv(otelResourceAttributesEnvVar, tt.input)

			// Clean the attributes
			cleanOTELResourceAttributes()

			// Check the result
			result := os.Getenv(otelResourceAttributesEnvVar)
			assert.Equal(t, tt.expected, result, "cleanOTELResourceAttributes() should clean attributes correctly")

			// Verify no empty values remain
			if result != "" {
				parts := strings.Split(result, ",")
				for i, part := range parts {
					assert.NotEmpty(t, part, "Found empty part at index %d", i)
					assert.False(t, strings.HasSuffix(part, "="), "Found empty value in part %q at index %d", part, i)
					// Verify it's a valid key=value pair
					assert.Contains(t, part, "=", "Invalid format (missing =) in part %q at index %d", part, i)
				}
			}
		})
	}
}

// TestDefaultResourceWithProblematicAttributes verifies that DefaultResource
// successfully creates a resource even with problematic OTEL_RESOURCE_ATTRIBUTES
// that would normally cause "partial resource: missing value: []" errors.
func TestDefaultResourceWithProblematicAttributes(t *testing.T) {
	// Save singleton state
	originalResource := defaultResource

	// Reset singleton to ensure our test runs fresh
	defaultResource = nil
	defaultResourceOnce = sync.Once{}

	t.Cleanup(func() {
		// Restore original singleton state
		defaultResource = originalResource
		defaultResourceOnce = sync.Once{}
	})

	// Set the exact problematic attributes from Sentry issue
	// This reproduces the exact error case where COG_VERSION_OVERRIDE is empty
	problematicAttrs := "compute_unit=gpu,compute_unit_count=1,deployable.key=dp-38f7b282eeb0429f8ec38901d1899ad0,deployment.key=dp-38f7b282eeb0429f8ec38901d1899ad0,docker_image_uri=r8.im/test/model@sha256:abc123,hardware=nvidia-a40,k8s.container.name=director,k8s.namespace.name=models,k8s.node.name=10.128.0.10,k8s.pod.name=model-dp-38f7b282eeb0429f8ec38901d1899ad0-7d8c544fd9-k55v5,model.full_name=test%2Fmodel,model.id=test%2Fmodel:abc123,model.name=model,model.owner=test,model_container.cog_version=0.12.2,model_container.cog_version_override=,model_container.cog_version_override_raw=%3E=0.11.3,version.id=abc123,"

	t.Setenv(otelResourceAttributesEnvVar, problematicAttrs)

	// This should not panic or error out
	resource := DefaultResource()

	// Verify resource was created successfully
	require.NotNil(t, resource, "DefaultResource() should not return nil")

	// Verify the attributes were cleaned
	cleanedAttrs := os.Getenv(otelResourceAttributesEnvVar)
	t.Logf("Original attributes: %q", problematicAttrs)
	t.Logf("Cleaned attributes: %q", cleanedAttrs)

	// Check that problematic empty values were removed
	assert.NotContains(t, cleanedAttrs, "model_container.cog_version_override=,",
		"Empty value 'model_container.cog_version_override=,' should be removed")
	assert.False(t, strings.HasSuffix(cleanedAttrs, ","), "Trailing comma should be removed")

	// Verify all parts are valid
	parts := strings.Split(cleanedAttrs, ",")
	for i, part := range parts {
		assert.NotEmpty(t, part, "Found empty part at index %d", i)
		assert.False(t, strings.HasSuffix(part, "="), "Found empty value in part %q at index %d", part, i)
	}
}
