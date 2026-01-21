// Package digest implements support for HTTP Content-Digest headers as
// described in [RFC 9530]. Currently it only supports adding SHA-512 digests to
// outgoing requests via the Transport type.
//
// [RFC 9530]: https://www.rfc-editor.org/rfc/rfc9530.html
package digest

import (
	"bytes"
	"crypto/sha512"
	"encoding/base64"
	"hash"
	"io"
	"net/http"
	"sync"
)

// Transport is an implementation of http.RoundTripper that automatically adds
// an RFC 9530 Content-Digest header to outgoing requests.
//
// Note: This transport will necessarily buffer the request body in memory in
// order to calculate the digest.
type Transport struct {
	http.RoundTripper

	bufPool  sync.Pool
	hashPool sync.Pool
}

func NewTransport(t http.RoundTripper) *Transport {
	return &Transport{
		RoundTripper: t,

		bufPool: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
		hashPool: sync.Pool{
			New: func() any {
				return sha512.New()
			},
		},
	}
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	h := t.hashPool.Get().(hash.Hash)
	h.Reset()
	defer t.hashPool.Put(h)

	// RoundTrip must not modify the original request.
	req = req.Clone(req.Context())

	if req.Body != nil {
		// RoundTrip must close the request body even in the event of an error.
		defer req.Body.Close()

		body := io.TeeReader(req.Body, h)

		buf := t.bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer t.bufPool.Put(buf)
		if _, err := io.Copy(buf, body); err != nil {
			return nil, err
		}

		req.Body = io.NopCloser(buf)
	}

	digest := base64.StdEncoding.EncodeToString(h.Sum(nil))
	req.Header.Set("Content-Digest", "sha-512=:"+digest+":")

	return t.RoundTripper.RoundTrip(req)
}
