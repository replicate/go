package uuid

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkNewV7(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = NewV7()
	}
}

func TestNewV7(t *testing.T) {
	n := 100_000
	uuids := make([]UUID, n)
	timestamps := make([]time.Time, n)

	start := time.Now()

	for i := 0; i < n; i++ {
		u, err := NewV7()
		require.Equal(t, u.Version(), V7)
		require.Equal(t, u.Variant(), VariantRFC4122)
		require.NoError(t, err)
		uuids[i] = u

		ts, err := TimeFromV7(u)
		require.NoError(t, err)
		timestamps[i] = ts
	}

	stop := time.Now()

	assert.IsNonDecreasing(t, timestamps)
	assert.WithinDuration(t, start, timestamps[0], 1*time.Millisecond)
	assert.WithinDuration(t, stop, timestamps[n-1], 1*time.Millisecond)
}
