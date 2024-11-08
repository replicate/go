// Package ptr contains a helper function for creating pointers to values.
package ptr

// To returns a pointer to a shallow copy of v.
func To[T any](v T) *T {
	return &v
}
