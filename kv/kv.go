package kv

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/hashicorp/go-rootcerts"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/replicate/go/logging"
	"github.com/replicate/go/telemetry"
)

var (
	logger       = logging.New("kv")
	errEmptyAddr = errors.New("empty Addr field")
)

// ClientOption allows configuration of Redis client options.
type ClientOption interface {
	apply(string, *redis.UniversalOptions) error
}

type clientOptionFunc func(string, *redis.UniversalOptions) error

func (fn clientOptionFunc) apply(name string, uOpts *redis.UniversalOptions) error {
	return fn(name, uOpts)
}

// Noop returns a no-operation client option.
func Noop() ClientOption {
	return clientOptionFunc(func(string, *redis.UniversalOptions) error {
		return nil
	})
}

// WithPoolSize sets the connection pool size for the Redis client.
func WithPoolSize(size int) ClientOption {
	log := logger.Sugar()

	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions) error {
		log.Infow("setting pool size for client", "client_name", name, "pool_size", size)
		uOpts.PoolSize = size

		return nil
	})
}

// WithSentinel configures Redis Sentinel support for high availability.
func WithSentinel(primaryName string, addrs []string, password string) ClientOption {
	log := logger.Sugar()

	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions) error {
		if primaryName == "" {
			log.Infow("no primary name set; not configuring", "client_name", name)

			return nil
		}

		log.Infow(
			"setting primary name, addrs, and sentinel password for client",
			"client_name", name,
			"primary_name", primaryName,
			"addrs", addrs,
		)

		uOpts.MasterName = primaryName
		uOpts.Addrs = addrs
		uOpts.SentinelPassword = password

		return nil
	})
}

// WithAutoTLS configures TLS with automatic certificate verification using the provided CA file.
func WithAutoTLS(caFile string) ClientOption {
	log := logger.Sugar()

	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions) error {
		if uOpts.TLSConfig == nil {
			log.Infow("no tls config present; not configuring", "client_name", name)

			return nil
		}

		pool, err := rootcerts.LoadCACerts(&rootcerts.Config{CAFile: caFile})
		if err != nil {
			return fmt.Errorf("failed to load certs from CA file %q: %w", caFile, err)
		}

		log.Infow("setting tls config for client", "client_name", name)

		uOpts.TLSConfig = &tls.Config{
			// Set InsecureSkipVerify to skip default validation which includes server
			// name verification. We *only* want to validate the issued cert against
			// the given CA pool. This will not disable VerifyConnection.
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			VerifyConnection: func(cs tls.ConnectionState) error {
				log.Debugw(
					"verifying connection",
					"server_name", cs.ServerName,
					"n_peer_certs", len(cs.PeerCertificates),
				)

				verOpts := x509.VerifyOptions{
					DNSName:       "", // skip name verification.
					Intermediates: x509.NewCertPool(),
					Roots:         pool,
				}

				for _, cert := range cs.PeerCertificates[1:] {
					verOpts.Intermediates.AddCert(cert)
				}

				if _, err := cs.PeerCertificates[0].Verify(verOpts); err != nil {
					return fmt.Errorf(
						"failed to verify peer certificate issuer=%q subject=%q: %w",
						cs.PeerCertificates[0].Issuer.String(),
						cs.PeerCertificates[0].Subject.String(),
						err,
					)
				}

				return nil
			},
		}

		return nil
	})
}

// optionsToUniversalOptions converts redis.Options to redis.UniversalOptions.
func optionsToUniversalOptions(opts *redis.Options) (*redis.UniversalOptions, error) {
	if opts.Addr == "" {
		return nil, errEmptyAddr
	}

	return &redis.UniversalOptions{
		Addrs:    []string{opts.Addr},
		DB:       opts.DB,
		Username: opts.Username,
		Password: opts.Password,

		// dial & hooks
		Dialer:    opts.Dialer,
		OnConnect: opts.OnConnect,

		// protocol/retries
		Protocol:        opts.Protocol,
		MaxRetries:      opts.MaxRetries,
		MinRetryBackoff: opts.MinRetryBackoff,
		MaxRetryBackoff: opts.MaxRetryBackoff,

		// timeouts
		DialTimeout:           opts.DialTimeout,
		ReadTimeout:           opts.ReadTimeout,
		WriteTimeout:          opts.WriteTimeout,
		ContextTimeoutEnabled: opts.ContextTimeoutEnabled,

		// pool
		PoolFIFO:        opts.PoolFIFO,
		PoolSize:        opts.PoolSize,
		PoolTimeout:     opts.PoolTimeout,
		MinIdleConns:    opts.MinIdleConns,
		MaxIdleConns:    opts.MaxIdleConns,
		MaxActiveConns:  opts.MaxActiveConns,
		ConnMaxIdleTime: opts.ConnMaxIdleTime,
		ConnMaxLifetime: opts.ConnMaxLifetime,

		// TLS & misc
		TLSConfig:       opts.TLSConfig,
		DisableIdentity: opts.DisableIdentity,
		UnstableResp3:   opts.UnstableResp3,
	}, nil
}

// New creates a new Redis client from a URL with the given options.
// Returns a redis.UniversalClient interface which provides access to the underlying client type.
//
// The client is automatically instrumented with OpenTelemetry tracing and tested
// with a ping operation before being returned.
//
// Example usage:
//
//	client, err := kv.New(
//		ctx,
//		"my-service",
//		"redis://localhost:6379",
//		kv.WithSentinel("mymaster", []string{"sentinel1:26379"}, "password"),
//		kv.WithAutoTLS("/path/to/ca.crt"),
//		kv.WithPoolSize(10),
//	)
func New(ctx context.Context, name, urlString string, clientOpts ...ClientOption) (redis.UniversalClient, error) {
	log := logger.Sugar()

	opts, err := redis.ParseURL(urlString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis URL (%s): %w", name, err)
	}

	uOpts, err := optionsToUniversalOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to convert redis options to universal options: %w", err)
	}

	for _, co := range clientOpts {
		if err := co.apply(name, uOpts); err != nil {
			return nil, err
		}
	}

	samplingTracerProvider, err := telemetry.CreateTracerProvider(
		context.Background(),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.01)),
	)
	if err != nil {
		return nil, err
	}

	client := redis.NewUniversalClient(uOpts)

	if err := redisotel.InstrumentTracing(
		client,
		redisotel.WithAttributes(attribute.String("client.name", name)),
		redisotel.WithTracerProvider(samplingTracerProvider),
	); err != nil {
		return nil, err
	}

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	log.Infow(
		"Built redis client",
		"client_name", name,
		"addrs", uOpts.Addrs,
		"client_type", fmt.Sprintf("%T", client),
	)

	return client, nil
}

// Exists checks if a key exists in Redis. Returns true if the key exists, false otherwise.
// May return an error if it cannot communicate with Redis.
func Exists(ctx context.Context, client redis.UniversalClient, key string) (bool, error) {
	result, err := client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}
