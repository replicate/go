package types_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/replicate/go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzParseDuration(f *testing.F) {
	f.Fuzz(func(t *testing.T, i int) {
		in := types.Duration(i)
		out, err := types.ParseDuration(in.String())
		require.NoError(t, err)
		assert.Equal(t, in.String(), out.String())
		assert.Equal(t, i, int(out))
	})
}

func TestParseDuration(t *testing.T) {
	for i, tc := range []struct {
		str string
		err error
		out float64
	}{
		{"PT00M", nil, 0},
		{"PT00H", nil, 0},
		{"PT1.5M", nil, 90},
		{"PT0.5H", nil, 1800},
		{"PT0.5H29M60S", nil, 3600},
		{"PT15S", nil, 15},
		{"PT1M", nil, 60},
		{"PT3M", nil, 180},
		{"PT130S", nil, 130},
		{"PT2M10S", nil, 130},
		{"P1DT2S", nil, 86402},
		{"PT5M10S", nil, 310},
		{"PT1H30M5S", nil, 5405},
		{"P2DT1H10S", nil, 176410},
		{"P14DT00H00M00S", nil, 1209600},
		{"PT1004199059S", nil, 1004199059},
		{"P3DT5H20M30.123S", nil, 278430.123},
		{"P1W", nil, 604800},
		{"P0.123W", nil, 74390.4},
		{"P1WT5S", nil, 604805},
		{"P1WT1H", nil, 608400},
		{"-P1WT1H", nil, -608400},
		{"-P1DT2S", nil, -86402},
		{"-PT1M5S", nil, -65},
		{"-P0.123W", nil, -74390.4},
		{"P0Y1W", nil, 604800},     // allow years if zero
		{"P0.0Y0M1W", nil, 604800}, // allow months if zero

		// Invalid formats
		{"P1M2Y", types.ErrInvalidDurationString, 0},     // wrong order
		{"P-1Y", types.ErrInvalidDurationString, 0},      // invalid sign
		{"P1YT-1M", types.ErrInvalidDurationString, 0},   // invalid sign
		{"P1S", types.ErrInvalidDurationString, 0},       // missing "T"
		{"1Y", types.ErrInvalidDurationString, 0},        // missing "P"
		{"P1YM5D", types.ErrInvalidDurationString, 0},    // no month value
		{" PT5M10S", types.ErrInvalidDurationString, 0},  // whitespace
		{"PT5M10S ", types.ErrInvalidDurationString, 0},  // whitespace
		{" PT5M10S ", types.ErrInvalidDurationString, 0}, // whitespace
		{"WOOFWOOF", types.ErrInvalidDurationString, 0},
		{"", types.ErrInvalidDurationString, 0},
		{"P", types.ErrInvalidDurationString, 0},
		{"PT", types.ErrInvalidDurationString, 0},
		{"P1Y2M3DT", types.ErrInvalidDurationString, 0},

		// Unsupported
		{"P1Y", types.ErrUnsupportedDurationString, 0},         // contains years
		{"P1Y24D", types.ErrUnsupportedDurationString, 0},      // contains years
		{"P1Y24DT6H", types.ErrUnsupportedDurationString, 0},   // contains years
		{"P1Y2M24DT6H", types.ErrUnsupportedDurationString, 0}, // contains years
		{"P1M", types.ErrUnsupportedDurationString, 0},         // contains months
		{"P1M1D", types.ErrUnsupportedDurationString, 0},       // contains months
		{"P1M2DT7H", types.ErrUnsupportedDurationString, 0},    // contains months
	} {

		d, err := types.ParseDuration(tc.str)
		if tc.err != nil {
			assert.ErrorIsf(t, err, tc.err, "[%d] ParseDuration(%q) -> %v, want %v", i, tc.str, err, tc.err)
			continue
		}

		val := time.Duration(d).Seconds()
		assert.Equalf(t, tc.out, val, "[%d] ParseDuration(%q) -> %f seconds, want %f", i, tc.str, val, tc.out)
	}
}

func TestDurationUnmarshalJSON(t *testing.T) {
	var d types.Duration

	err := json.Unmarshal([]byte(`"P14DT00H00M400.123S"`), &d)
	require.NoError(t, err)

	assert.Equal(t, 1210000.123, time.Duration(d).Seconds())
}

func TestDurationMarshalJSON(t *testing.T) {
	d := 73*time.Hour + 14*time.Minute + 46*time.Second + 789*time.Millisecond

	result, err := json.Marshal(types.Duration(d))
	require.NoError(t, err)

	assert.Equal(t, `"P3DT1H14M46.789S"`, string(result))
}
