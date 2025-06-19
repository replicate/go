package kv

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/hashicorp/go-rootcerts"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/replicate/go/logging"
	"github.com/replicate/go/telemetry"
)

var (
	logger       = logging.New("kv")
	errEmptyAddr = errors.New("empty Addr field")

	// Global tracer provider reuse
	globalTracerProvider trace.TracerProvider
	tracerProviderOnce   sync.Once

	// Default pool sizes based on environment
	defaultPoolSizes = map[string]int{
		"production":  20,
		"staging":     15,
		"development": 10,
		"test":        5,
	}
)

// ClientOption allows configuration of Redis client options.
type ClientOption interface {
	apply(string, *redis.UniversalOptions, *clientConfig) error
}

// clientConfig holds additional configuration not part of redis.UniversalOptions
type clientConfig struct {
	traceSamplingRate *float64
	tracerProvider    trace.TracerProvider
	enhancedLogging   bool
}

type clientOptionFunc func(string, *redis.UniversalOptions, *clientConfig) error

func (fn clientOptionFunc) apply(name string, uOpts *redis.UniversalOptions, cfg *clientConfig) error {
	return fn(name, uOpts, cfg)
}

// Noop returns a no-operation client option.
func Noop() ClientOption {
	return clientOptionFunc(func(string, *redis.UniversalOptions, *clientConfig) error {
		return nil
	})
}

// WithPoolSize sets the connection pool size for the Redis client.
func WithPoolSize(size int) ClientOption {
	log := logger.Sugar()

	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions, cfg *clientConfig) error {
		if size <= 0 {
			return fmt.Errorf("pool size must be positive for client %q, got %d", name, size)
		}
		if size > 1000 {
			log.Warnw("large pool size detected", "client_name", name, "pool_size", size)
		}

		log.Infow("setting pool size for client", "client_name", name, "pool_size", size)
		uOpts.PoolSize = size

		return nil
	})
}

// WithSentinel configures Redis Sentinel support for high availability.
func WithSentinel(primaryName string, addrs []string, password string) ClientOption {
	log := logger.Sugar()

	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions, cfg *clientConfig) error {
		if primaryName == "" {
			log.Infow("no primary name set; not configuring sentinel", "client_name", name)
			return nil
		}

		if len(addrs) == 0 {
			return fmt.Errorf("sentinel addresses cannot be empty for client %q", name)
		}

		log.Infow(
			"configuring sentinel for client",
			"client_name", name,
			"primary_name", primaryName,
			"addrs", addrs,
			"password_set", password != "",
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

	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions, cfg *clientConfig) error {
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
						"TLS verification failed for client %q (server=%q, issuer=%q, subject=%q): %w",
						name,
						cs.ServerName,
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

// WithTraceSampling sets the OpenTelemetry trace sampling rate for the Redis client.
func WithTraceSampling(rate float64) ClientOption {
	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions, cfg *clientConfig) error {
		if rate < 0 || rate > 1 {
			return fmt.Errorf("trace sampling rate must be between 0 and 1 for client %q, got %f", name, rate)
		}
		cfg.traceSamplingRate = &rate
		return nil
	})
}

// WithTracerProvider allows injection of a custom OpenTelemetry tracer provider.
func WithTracerProvider(provider trace.TracerProvider) ClientOption {
	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions, cfg *clientConfig) error {
		if provider == nil {
			return fmt.Errorf("tracer provider cannot be nil for client %q", name)
		}
		cfg.tracerProvider = provider
		return nil
	})
}

// WithEnhancedLogging enables more detailed logging for the Redis client.
func WithEnhancedLogging(enabled bool) ClientOption {
	return clientOptionFunc(func(name string, uOpts *redis.UniversalOptions, cfg *clientConfig) error {
		cfg.enhancedLogging = enabled
		return nil
	})
}

// getDefaultPoolSize returns environment-appropriate default pool size.
func getDefaultPoolSize() int {
	env := os.Getenv("ENVIRONMENT")
	if env == "" {
		env = "development"
	}

	if size, exists := defaultPoolSizes[env]; exists {
		return size
	}
	return defaultPoolSizes["development"]
}

// getTraceSamplingRate returns the trace sampling rate from environment or default.
func getTraceSamplingRate() float64 {
	if rateStr := os.Getenv("REDIS_TRACE_SAMPLING_RATE"); rateStr != "" {
		if rate, err := strconv.ParseFloat(rateStr, 64); err == nil && rate >= 0 && rate <= 1 {
			return rate
		}
	}
	return 0.01 // Default 1% sampling
}

// getGlobalTracerProvider returns a shared tracer provider instance.
func getGlobalTracerProvider() (trace.TracerProvider, error) {
	var err error
	tracerProviderOnce.Do(func() {
		globalTracerProvider, err = telemetry.CreateTracerProvider(
			context.Background(),
			sdktrace.WithSampler(sdktrace.TraceIDRatioBased(getTraceSamplingRate())),
		)
	})
	return globalTracerProvider, err
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
// Returns a redis.Cmdable interface for maximum compatibility across different use cases.
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
func New(ctx context.Context, name, urlString string, clientOpts ...ClientOption) (redis.Cmdable, error) {
	log := logger.Sugar()

	if name == "" {
		return nil, fmt.Errorf("client name cannot be empty")
	}
	if urlString == "" {
		return nil, fmt.Errorf("redis URL cannot be empty for client %q", name)
	}

	opts, err := redis.ParseURL(urlString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis URL for client %q: %w", name, err)
	}

	uOpts, err := optionsToUniversalOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to convert redis options to universal options for client %q: %w", name, err)
	}

	// Set default pool size if not already set
	if uOpts.PoolSize == 0 {
		uOpts.PoolSize = getDefaultPoolSize()
	}

	// Initialize client configuration
	cfg := &clientConfig{}

	// Apply all client options
	for _, co := range clientOpts {
		if err := co.apply(name, uOpts, cfg); err != nil {
			return nil, fmt.Errorf("failed to apply client option for client %q: %w", name, err)
		}
	}

	// Get or create tracer provider
	var tracerProvider trace.TracerProvider
	if cfg.tracerProvider != nil {
		tracerProvider = cfg.tracerProvider
	} else {
		tracerProvider, err = getGlobalTracerProvider()
		if err != nil {
			return nil, fmt.Errorf("failed to create tracer provider for client %q: %w", name, err)
		}
	}

	// Override sampling rate if specified
	if cfg.traceSamplingRate != nil {
		tracerProvider, err = telemetry.CreateTracerProvider(
			context.Background(),
			sdktrace.WithSampler(sdktrace.TraceIDRatioBased(*cfg.traceSamplingRate)),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create custom tracer provider for client %q: %w", name, err)
		}
	}

	client := redis.NewUniversalClient(uOpts)

	if err := redisotel.InstrumentTracing(
		client,
		redisotel.WithAttributes(attribute.String("client.name", name)),
		redisotel.WithTracerProvider(tracerProvider),
	); err != nil {
		return nil, fmt.Errorf("failed to instrument tracing for client %q: %w", name, err)
	}

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping Redis for client %q: %w", name, err)
	}

	// Enhanced logging if requested
	if cfg.enhancedLogging {
		log.Infow(
			"redis client created with enhanced logging",
			"client_name", name,
			"addrs", uOpts.Addrs,
			"db", uOpts.DB,
			"pool_size", uOpts.PoolSize,
			"sentinel_enabled", uOpts.MasterName != "",
			"tls_enabled", uOpts.TLSConfig != nil,
			"client_type", fmt.Sprintf("%T", client),
		)
	} else {
		log.Infow(
			"redis client created",
			"client_name", name,
			"addrs", uOpts.Addrs,
			"client_type", fmt.Sprintf("%T", client),
		)
	}

	return client, nil
}
