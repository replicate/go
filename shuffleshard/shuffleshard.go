package shuffleshard

import (
	"hash/fnv"
	"math/rand"
)

// Get implements a basic shuffle shard algorithm. Given a specified number of
// underlying items, it hashes the provided key and uses the resulting hash to
// select a number of items specified by the count parameter.
//
// For a given hash key, the same shard of items will always be selected.
//
// For two different hash keys and appropriate values of items and count, a
// fully overlapping set of items is unlikely. The probability of a full
// collision between any two keys is roughly 1/(items choose count).
func Get(items, count int, key []byte) []int {
	h := fnv.New64a()
	h.Write(key)
	rng := rand.New(rand.NewSource(int64(h.Sum64())))

	return rng.Perm(items)[:count]
}
