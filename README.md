# `replicate/go`

This repository contains shared packages used by Go applications at Replicate.
If we find ourselves writing the same code repeatedly across different
applications, we might want to put it here.

In due course, it may become a monorepo for our services and tools built using
Go.

For now, the repository is public, to simplify its use as a Go module in other
applications.

## Packages

### `cache`

A redis-backed typed object cache implementation supporting serve-from-stale and
asynchronous cache fills.

### `errors`

Configures conventions for recording errors: a wrapper around the Sentry SDK.

### `flags`

Feature flagging functions: a thin wrapper around the LaunchDarkly client.

### `httpclient`

Conventions for creating HTTP clients with appropriate pooling and timeout
configuration. Heavily inspired by <https://github.com/hashicorp/go-cleanhttp>.

### `lock`

A redis-backed distributed lock for coordination within multi-instance services.

### `logging`

Configures logging conventions for `github.com/sirupsen/logrus`.

### `ratelimit`

A redis-backed token-bucket rate limiter implementation.

### `telemetry`

Standard OpenTelemetry configuration and tracer creation.

### `version`

A convention for retrieving service version from the environment.

## Development

You can run the build, check the tests, lint the code, and run a formatter using
the scripts provided in `script/`

    script/build
    script/test
    script/format
    script/lint
