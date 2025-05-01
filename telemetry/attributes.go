package telemetry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrUnsupportedValue      = fmt.Errorf("unsupported value")
	ErrUnsupportedSliceValue = fmt.Errorf("%w: slice attributes may contain only one type", ErrUnsupportedValue)
)

// Attributes is a wrapper around a slice of attribute.KeyValue values which
// serializes to and from a simple JSON dictionary, handling type validation and
// ensuring that JSON numbers are upcast to the appropriate types in the
// attributes (int64 if possible, float64 otherwise).
type Attributes []attribute.KeyValue

func (as Attributes) AsSlice() []attribute.KeyValue {
	return []attribute.KeyValue(as)
}

func (as Attributes) MarshalJSON() ([]byte, error) {
	attrMap := make(map[string]any)
	for _, a := range as {
		attrMap[string(a.Key)] = a.Value.AsInterface()
	}
	return json.Marshal(attrMap)
}

func (as *Attributes) UnmarshalJSON(b []byte) error {
	var attrMap map[string]any

	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber() // read JSON numbers into json.Number so we can distinguish int/float

	if err := d.Decode(&attrMap); err != nil {
		return err
	}

	kvs := make([]attribute.KeyValue, 0, len(attrMap))

	for k, v := range attrMap {
		key := attribute.Key(k)
		value, err := getValue(v)
		if errors.Is(err, ErrUnsupportedValue) {
			logger.Sugar().Warnw("skipping unsupported attribute value", "key", k, "error", err)
			continue
		} else if err != nil {
			return err
		}
		kvs = append(kvs, attribute.KeyValue{Key: key, Value: value})
	}
	sort.Slice(kvs, func(i, j int) bool {
		return string(kvs[i].Key) < string(kvs[j].Key)
	})

	*as = kvs

	return nil
}

func getValue(value any) (attribute.Value, error) {
	switch v := value.(type) {
	case json.Number:
		if asInt64, err := v.Int64(); err == nil {
			return attribute.Int64Value(asInt64), nil
		}
		//nolint:revive
		if asFloat64, err := v.Float64(); err == nil {
			return attribute.Float64Value(asFloat64), nil
		} else {
			return attribute.Value{}, err
		}
	case bool:
		return attribute.BoolValue(v), nil
	case string:
		return attribute.StringValue(v), nil
	case []any:
		return getSliceValue(v)
	default:
		return attribute.Value{}, ErrUnsupportedValue
	}
}

func getSliceValue(values []any) (attribute.Value, error) {
	if len(values) == 0 {
		// We have no type information, we arbitrarily decide it's a string slice.
		return attribute.StringSliceValue([]string{}), nil
	}

	var isFloat bool

	if _, ok := values[0].(json.Number); ok {
		// If it's a json.Number, then we only map to int64 if *all* the values can
		// be parsed as ints.
		for _, v := range values {
			asNumber, ok := v.(json.Number)
			if !ok {
				return attribute.Value{}, ErrUnsupportedSliceValue
			}
			if _, err := asNumber.Int64(); err != nil {
				isFloat = true
				break
			}
		}
	}

	switch values[0].(type) {
	case json.Number:
		if isFloat {
			s := make([]float64, len(values))
			for i, v := range values {
				asNumber, ok := v.(json.Number)
				if !ok {
					return attribute.Value{}, ErrUnsupportedSliceValue
				}
				asFloat64, err := asNumber.Float64()
				if err != nil {
					return attribute.Value{}, err
				}
				s[i] = asFloat64
			}
			return attribute.Float64SliceValue(s), nil
		}
		s := make([]int64, len(values))
		for i, v := range values {
			asNumber, ok := v.(json.Number)
			if !ok {
				return attribute.Value{}, ErrUnsupportedSliceValue
			}
			asInt64, err := asNumber.Int64()
			if err != nil {
				return attribute.Value{}, err
			}
			s[i] = asInt64
		}
		return attribute.Int64SliceValue(s), nil
	case bool:
		s, err := createTypedSlice[bool](values)
		if err != nil {
			return attribute.Value{}, err
		}
		return attribute.BoolSliceValue(s), nil
	case string:
		s, err := createTypedSlice[string](values)
		if err != nil {
			return attribute.Value{}, err
		}
		return attribute.StringSliceValue(s), nil
	default:
		return attribute.Value{}, ErrUnsupportedValue
	}
}

func createTypedSlice[T any](values []any) ([]T, error) {
	s := make([]T, len(values))
	for i, v := range values {
		asT, ok := v.(T)
		if !ok {
			return nil, ErrUnsupportedSliceValue
		}
		s[i] = asT
	}
	return s, nil
}
