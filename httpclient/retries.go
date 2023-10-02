package httpclient

import (
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// ApplyRetryPolicy wraps an HTTPClient with one that has a retry policy.
// Currently the retry policy will retry in any of the following scenarios:
//
// - connection errors and timeouts
// - 429 or 503 responses (Retry-After is respected)
// - other 5XX responses (except 501)
//
// All other responses are not retried.
//
// With a minimum wait time of 100ms, and a maximum of 4 retries, the
// inter-retry intervals will be 100ms, 200ms, 400ms, 800ms with the default
// backoff policy, for a total time of 1500ms (excluding request attempt times).
func ApplyRetryPolicy(c *http.Client) *http.Client {
	retryClient := &retryablehttp.Client{
		HTTPClient:   c,
		Logger:       nil, // "logging" is provided by OTel transport on the web client
		RetryWaitMin: 100 * time.Millisecond,
		RetryWaitMax: 2 * time.Second,
		RetryMax:     4,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      retryablehttp.DefaultBackoff,
	}
	return retryClient.StandardClient()
}
