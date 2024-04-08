// Package must helps you do things that must not fail.
//
// Example:
//
//	var clusterURL = must.Get(url.Parse(...))
//	var conn = must.Get(net.Dial("tcp", ...))
//	must.Do(telemetry.Shutdown())
package must

// Do panics if err is non-nil.
func Do(err error) {
	if err != nil {
		panic(err)
	}
}

// Get returns v, and panics if err is non-nil.
func Get[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
