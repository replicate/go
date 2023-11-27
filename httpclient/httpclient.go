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
	"golang.org/x/net/http2"
)

const ConnectTimeout = 5 * time.Second

// DefaultRoundTripper returns an http.RoundTripper with similar default values
// to http.DefaultTransport, but with idle connections and keepalives disabled.
// The transport is configured to emit OTel spans.
func DefaultRoundTripper() http.RoundTripper {
	transport := DefaultPooledTransport()
	transport.DisableKeepAlives = true
	transport.MaxIdleConnsPerHost = -1
	return otelhttp.NewTransport(transport)
}

// DefaultPooledRoundTripper returns an http.RoundTripper with similar default
// values to http.DefaultTransport. Do not use this for transient transports as
// it can leak file descriptors over time. Only use this for transports that
// will be re-used for the same host(s).
func DefaultPooledRoundTripper() http.RoundTripper {
	return otelhttp.NewTransport(DefaultPooledTransport())
}

// PooledEgressRoundTripper returns an http.RoundTripper designed to call
// arbitrary 3rd-party endpoints. It accepts a proxy function which in
// production should point to a suitable egress proxy.
func PooledEgressRoundTripper(proxy func(*http.Request) (*url.URL, error)) http.RoundTripper {
	transport := DefaultPooledTransport()
	transport.Proxy = proxy

	// Set a no-op propagator that won't forward any trace info.
	noopPropagator := propagation.NewCompositeTextMapPropagator()

	return otelhttp.NewTransport(transport, otelhttp.WithPropagators(noopPropagator))
}

// DefaultPooledTransport returns a new http.Transport with similar default
// values to http.DefaultTransport. Do not use this for transient transports as
// it can leak file descriptors over time. Only use this for transports that
// will be re-used for the same host(s).
//
// You should usually use DefaultPooledRoundTripper instead. If you do use this
// (perhaps in order to tweak some of the configuration values) be aware that
// you will need to handle wrapping it in an OTel transport with
// `otelhttp.NewTransport` yourself.
func DefaultPooledTransport() *http.Transport {
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
	configureHTTP2(transport)
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

func configureHTTP2(t *http.Transport) {
	h2t, err := http2.ConfigureTransports(t)
	if err != nil {
		// ConfigureTransports should only ever return an error if the transport
		// passed in has already been configured for http2, which shouldn't be
		// possible for us.
		panic(err)
	}

	// Send a ping frame on any connection that's been idle for more than 10
	// seconds.
	//
	// The default is to never do this. We set it primarily as a workaround for
	//
	//   https://github.com/golang/go/issues/59690
	//
	// where a connection that goes AWOL will not be correctly terminated and
	// removed from the connection pool under certain circumstances. Together
	// `ReadIdleTimeout` and `PingTimeout` should ensure that we remove defunct
	// connections in ~20 seconds.
	h2t.ReadIdleTimeout = 10 * time.Second
	// Give the other end 10 seconds to respond. If we don't hear back, we'll
	// close the connection.
	h2t.PingTimeout = 10 * time.Second
}
