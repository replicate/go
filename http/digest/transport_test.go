package digest_test

import (
	"bytes"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/replicate/go/http/digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func serverExpectingDigest(t *testing.T, digest string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := "sha-512=:" + digest + ":"
		received := r.Header.Get("Content-Digest")

		assert.Equal(t, expected, received)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"OK"}`))
	}))
}

// Generate a predictable seeded random payload of a given size
func generatePayload(s1, s2 uint64, size int) io.ReadCloser {
	r := rand.New(rand.NewPCG(s1, s2))
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(r.IntN(256))
	}
	return io.NopCloser(bytes.NewReader(data))
}

func TestTransport(t *testing.T) {
	testcases := []struct {
		Name   string
		Body   io.ReadCloser
		Digest string
	}{
		{
			Name:   "nil body",
			Body:   nil,
			Digest: "z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg==",
		},
		{
			Name:   "empty body",
			Body:   io.NopCloser(bytes.NewReader([]byte{})),
			Digest: "z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg==",
		},
		{
			Name:   "hello world",
			Body:   io.NopCloser(bytes.NewReader([]byte("hello world"))),
			Digest: "MJ7MSJwS1utMxA9QyQLytNDtd+5RGnx6m808qG1M2G+YndNbxf9JlnDaNCVbRbDP2DDoH2Bdz33FVC6TrpzXbw==",
		},
		{
			Name:   "large body (128KB)",
			Body:   generatePayload(42, 42, 128*1024),
			Digest: "fV+7qAxDBpKPaXsFZogCBpSROb5F+j/5kvIIPWMXQUcyiOiL/4YCbo9HwybsuD1rYQ7sBAEW4HnlHrrkSYEI6w==",
		},
	}

	client := &http.Client{
		Transport: digest.NewTransport(http.DefaultTransport),
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			server := serverExpectingDigest(t, tc.Digest)
			defer server.Close()

			req, err := http.NewRequest("GET", server.URL, tc.Body)
			require.NoError(t, err)

			resp, err := client.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()
		})
	}
}

type nopTransport struct{}

func (tr *nopTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &http.Response{}, nil
}

func BenchmarkTransport(b *testing.B) {
	n := 128 * 1024

	b.ReportAllocs()
	b.SetBytes(int64(n))

	transport := digest.NewTransport(&nopTransport{})

	requests := make([]*http.Request, b.N)
	for i := 0; i < b.N; i++ {
		req, err := http.NewRequest("GET", "http://example.com", generatePayload(456, uint64(i), n))
		require.NoError(b, err)
		requests[i] = req
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = transport.RoundTrip(requests[i])
	}
}
