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

Configures logging conventions for [github.com/sirupsen/logrus](https://github.com/uber-go/zap).

### `ratelimit`

A redis-backed token-bucket rate limiter implementation.

### `telemetry`

Standard OpenTelemetry configuration and tracer creation.

### `version`

A convention for retrieving service version from the environment.

## Tools

The repository also contains a `uuid` command that can be used to generate a list
of uuids using the `uuid` package.

```bash
% go run ./cmd/uuid/main.go
> 018d3855-24f0-7531-84b4-4e88f13bab70
% go run ./cmd/uuid/main.go -timestamps
> 018d3855-24f0-7531-84b4-4e88f13bab70 2024-01-23T21:58:40.624Z
% go run ./cmd/uuid/main.go -count 5 -timestamps
> 018d3855-24f0-7531-84b4-4e88f13bab70 2024-01-23T21:58:40.624Z
> 018d3855-24f1-7b1e-be24-4ce73f77d3bf 2024-01-23T21:58:40.625Z
> 018d3855-24f1-7b33-8b7b-50e5e535b510 2024-01-23T21:58:40.625Z
> 018d3855-24f1-75d7-abb1-0db1ac9ac285 2024-01-23T21:58:40.625Z
> 018d3855-24f1-78fc-a1ce-a81e26b3fbe8 2024-01-23T21:58:40.625Z
```

## Development

You can run the build, check the tests, lint the code, and run a formatter using
the scripts provided in `script/`

    script/build
    script/test
    script/format
    script/lint
