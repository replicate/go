package shuffleshard

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetConsistency(t *testing.T) {
	items := 32
	count := 3

	for i := 0; i < 1000; i++ {
		key := make([]byte, 128)
		_, _ = rand.Read(key)

		selected := Get(items, count, key)

		assert.Len(t, selected, count)
		assert.Equal(t, selected, Get(items, count, key))
	}
}

func TestGetValuesInAppropriateRange(t *testing.T) {
	items := 32
	count := 3

	for i := 0; i < 1000; i++ {
		key := make([]byte, 128)
		_, _ = rand.Read(key)

		shard := Get(items, count, key)

		for _, item := range shard {
			assert.GreaterOrEqual(t, item, 0)
			assert.Less(t, item, items)
		}
	}
}

func TestGetUniqueValues(t *testing.T) {
	items := 32
	count := 8

	for i := 0; i < 1000; i++ {
		key := make([]byte, 128)
		_, _ = rand.Read(key)

		shard := Get(items, count, key)
		shardMap := make(map[int]struct{})

		for _, item := range shard {
			shardMap[item] = struct{}{}
		}

		assert.Len(t, shardMap, count)
	}
}
