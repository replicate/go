// Package httpclient collects conventions for the configuration of HTTP clients
// used across our various codebases.
//
// It is heavily inspired by github.com/hashicorp/go-cleanhttp.
package httpclient

import (
	"net"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
)

const ConnectTimeout = 5 * time.Second

// DefaultRoundTripper returns an http.RoundTripper with similar default values
// to http.DefaultTransport, but with idle connections and keepalives disabled.
// The transport is configured to emit OTel spans.
func DefaultRoundTripper() http.RoundTripper {
	transport := defaultPooledTransport()
	transport.DisableKeepAlives = true
	transport.MaxIdleConnsPerHost = -1
	return otelhttp.NewTransport(transport)
}

// DefaultPooledRoundTripper returns an http.RoundTripper with similar default
// values to http.DefaultTransport. Do not use this for transient transports as
// it can leak file descriptors over time. Only use this for transports that
// will be re-used for the same host(s).
func DefaultPooledRoundTripper() http.RoundTripper {
	return otelhttp.NewTransport(defaultPooledTransport())
}

// PooledEgressRoundTripper returns an http.RoundTripper designed to call
// arbitrary 3rd-party endpoints. It accepts a proxy function which in
// production should point to a suitable egress proxy.
func PooledEgressRoundTripper(proxy func(*http.Request) (*url.URL, error)) http.RoundTripper {
	transport := defaultPooledTransport()
	transport.Proxy = proxy

	// Set a no-op propagator that won't forward any trace info.
	noopPropagator := propagation.NewCompositeTextMapPropagator()

	return otelhttp.NewTransport(transport, otelhttp.WithPropagators(noopPropagator))
}

func defaultPooledTransport() *http.Transport {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   ConnectTimeout,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
	}
	return transport
}

// DefaultClient returns a new http.Client with similar default values to
// http.Client, but with a non-shared Transport, idle connections disabled, and
// keepalives disabled.
func DefaultClient() *http.Client {
	return &http.Client{
		Transport: DefaultRoundTripper(),
	}
}

// DefaultPooledClient returns a new http.Client with similar default values to
// http.Client, but with a shared Transport. Do not use this function for
// transient clients as it can leak file descriptors over time. Only use this
// for clients that will be re-used for the same host(s).
func DefaultPooledClient() *http.Client {
	return &http.Client{
		Transport: DefaultPooledRoundTripper(),
	}
}
