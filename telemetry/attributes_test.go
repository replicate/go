package telemetry

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

var attributeTestCases = []struct {
	Name string
	JSON string
	KVs  []attribute.KeyValue
}{
	{
		Name: "bool",
		JSON: `{"enabled": true, "is_this_thing_on": false}`,
		KVs: []attribute.KeyValue{
			attribute.Bool("enabled", true),
			attribute.Bool("is_this_thing_on", false),
		},
	},
	{
		Name: "int64",
		JSON: `{"age": 42, "zero": 0, "bigger_than_int32": 2147483650}`,
		KVs: []attribute.KeyValue{
			attribute.Int("age", 42),
			attribute.Int("zero", 0),
			attribute.Int("bigger_than_int32", 2147483650),
		},
	},
	{
		Name: "float64",
		JSON: `{"pi": 3.141592653589793, "negative": -1.234567}`,
		KVs: []attribute.KeyValue{
			attribute.Float64("pi", 3.141592653589793),
			attribute.Float64("negative", -1.234567),
		},
	},
	{
		Name: "string",
		JSON: `{"name": "Boz", "empty": ""}`,
		KVs: []attribute.KeyValue{
			attribute.String("name", "Boz"),
			attribute.String("empty", ""),
		},
	},
	{
		Name: "bool slice",
		JSON: `{"flags": [true, false, false, true]}`,
		KVs: []attribute.KeyValue{
			attribute.BoolSlice("flags", []bool{true, false, false, true}),
		},
	},
	{
		Name: "int64 slice",
		JSON: `{"lotto": [12, 17, 46]}`,
		KVs: []attribute.KeyValue{
			attribute.IntSlice("lotto", []int{12, 17, 46}),
		},
	},
	{
		Name: "float64 slice",
		JSON: `{"coordinates": [51.477928, -0.001545], "mixed": [1, 2, 3, 4.5]}`,
		KVs: []attribute.KeyValue{
			attribute.Float64Slice("coordinates", []float64{51.477928, -0.001545}),
			attribute.Float64Slice("mixed", []float64{1, 2, 3, 4.5}),
		},
	},
	{
		Name: "string slice",
		JSON: `{"hobbies": ["gardening", "fishing"], "empty": []}`,
		KVs: []attribute.KeyValue{
			attribute.StringSlice("hobbies", []string{"gardening", "fishing"}),
			attribute.StringSlice("empty", []string{}),
		},
	},
}

func TestAttributesMarshalFunctional(t *testing.T) {
	x := struct {
		Attributes Attributes `json:"my_attrs"`
	}{
		Attributes: Attributes([]attribute.KeyValue{
			attribute.Bool("enabled", true),
			attribute.Int("age", 42),
			attribute.Float64("pi", 3.141592653589793),
			attribute.String("name", "Florp"),
		}),
	}

	out, err := json.Marshal(x)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"my_attrs": {
			"enabled": true,
			"age": 42,
			"pi": 3.141592653589793,
			"name": "Florp"
		}
	}`, string(out))
}

func TestAttributesMarshal(t *testing.T) {
	for _, tc := range attributeTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			out, err := json.Marshal(Attributes(tc.KVs))
			require.NoError(t, err)

			assert.JSONEq(t, tc.JSON, string(out))
		})
	}
}

func TestAttributesUnmarshal(t *testing.T) {
	for _, tc := range attributeTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			var attrs Attributes

			err := json.Unmarshal([]byte(tc.JSON), &attrs)
			require.NoError(t, err)

			assert.ElementsMatch(t, tc.KVs, attrs)
		})
	}
}

// For now, we want to ignore rather than choke on invalid types.
func TestAttributesUnmarshalInvalidTypes(t *testing.T) {
	testCases := []struct {
		Name string
		JSON string
	}{
		{
			Name: "null",
			JSON: `{"null": null}`,
		},
		{
			Name: "empty map",
			JSON: `{"map": {}}`,
		},
		{
			Name: "map with entries",
			JSON: `{"map": {"name": "Chigozie"}}`,
		},
		{
			Name: "mixed type slice",
			JSON: `{"mixedup": [123, "eatmyshorts"]}`,
		},
		{
			Name: "nested slice",
			JSON: `{"nested": [[1], [2], [3]]}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var attrs Attributes

			err := json.Unmarshal([]byte(tc.JSON), &attrs)
			require.NoError(t, err)

			assert.Empty(t, attrs)
		})
	}
}
