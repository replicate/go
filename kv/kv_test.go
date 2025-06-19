package kv

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	// Skip if Redis is not available
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping integration tests")
	}

	ctx := context.Background()
	client, err := New(ctx, "test-client", "redis://localhost:6379")
	require.NoError(t, err)
	require.NotNil(t, client)

	// Test basic functionality
	err = client.Set(ctx, "test-key", "test-value", time.Minute).Err()
	require.NoError(t, err)

	val, err := client.Get(ctx, "test-key").Result()
	require.NoError(t, err)
	assert.Equal(t, "test-value", val)

	// Cleanup
	err = client.Del(ctx, "test-key").Err()
	require.NoError(t, err)
}

func TestNewWithInvalidURL(t *testing.T) {
	ctx := context.Background()
	client, err := New(ctx, "test-client", "invalid-url")
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "failed to parse redis URL")
}

func TestNewWithEmptyAddr(t *testing.T) {
	// Test a scenario where we can't connect (connection refused)
	ctx := context.Background()
	client, err := New(ctx, "test-client", "redis://localhost:9999")
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "failed to ping")
}

func TestWithPoolSize(t *testing.T) {
	// Skip if Redis is not available
	if !isRedisAvailable() {
		t.Skip("Redis not available, skipping integration tests")
	}

	ctx := context.Background()
	client, err := New(ctx, "test-client", "redis://localhost:6379", WithPoolSize(5))
	require.NoError(t, err)
	require.NotNil(t, client)

	// Test that client works with custom pool size
	err = client.Ping(ctx).Err()
	require.NoError(t, err)
}

func TestWithSentinel(t *testing.T) {
	// This is a unit test that doesn't require actual sentinel setup
	opts := &redis.UniversalOptions{}
	cfg := &clientConfig{}

	option := WithSentinel("mymaster", []string{"sentinel1:26379", "sentinel2:26379"}, "password")
	err := option.apply("test-client", opts, cfg)
	require.NoError(t, err)

	assert.Equal(t, "mymaster", opts.MasterName)
	assert.Equal(t, []string{"sentinel1:26379", "sentinel2:26379"}, opts.Addrs)
	assert.Equal(t, "password", opts.SentinelPassword)
}

func TestWithSentinelEmpty(t *testing.T) {
	opts := &redis.UniversalOptions{
		Addrs: []string{"redis:6379"}, // existing address
	}
	cfg := &clientConfig{}

	option := WithSentinel("", []string{"sentinel1:26379"}, "password")
	err := option.apply("test-client", opts, cfg)
	require.NoError(t, err)

	// Should not modify options when primary name is empty
	assert.Equal(t, "", opts.MasterName)
	assert.Equal(t, []string{"redis:6379"}, opts.Addrs) // unchanged
	assert.Equal(t, "", opts.SentinelPassword)
}

func TestWithAutoTLS(t *testing.T) {
	// Test with no TLS config
	opts := &redis.UniversalOptions{}
	cfg := &clientConfig{}

	option := WithAutoTLS("/path/to/ca.crt")
	err := option.apply("test-client", opts, cfg)
	require.NoError(t, err)

	// Should not modify options when TLSConfig is nil
	assert.Nil(t, opts.TLSConfig)
}

func TestWithAutoTLSWithExistingConfig(t *testing.T) {
	// Create a temporary CA file for testing
	tmpFile, err := os.CreateTemp("", "test-ca-*.crt")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Generate a test certificate at runtime
	cert, key, err := generateTestCert()
	require.NoError(t, err)

	_, err = tmpFile.WriteString(cert)
	require.NoError(t, err)
	tmpFile.Close()

	opts := &redis.UniversalOptions{
		TLSConfig: &tls.Config{}, // Signal that TLS is wanted
	}
	cfg := &clientConfig{}

	option := WithAutoTLS(tmpFile.Name())
	err = option.apply("test-client", opts, cfg)
	require.NoError(t, err)

	// Should have configured TLS
	assert.NotNil(t, opts.TLSConfig)
	assert.True(t, opts.TLSConfig.InsecureSkipVerify)
	assert.NotNil(t, opts.TLSConfig.VerifyConnection)

	_ = key // Silence unused variable warning
}

func TestNoop(t *testing.T) {
	opts := &redis.UniversalOptions{
		PoolSize: 10,
	}
	cfg := &clientConfig{}
	originalPoolSize := opts.PoolSize

	option := Noop()
	err := option.apply("test-client", opts, cfg)
	require.NoError(t, err)

	// Should not modify any options
	assert.Equal(t, originalPoolSize, opts.PoolSize)
}

func TestOptionsToUniversalOptions(t *testing.T) {
	tests := []struct {
		name    string
		opts    *redis.Options
		wantErr bool
	}{
		{
			name: "valid options",
			opts: &redis.Options{
				Addr:     "localhost:6379",
				DB:       1,
				Username: "user",
				Password: "pass",
				PoolSize: 10,
			},
			wantErr: false,
		},
		{
			name: "empty address",
			opts: &redis.Options{
				Addr: "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uOpts, err := optionsToUniversalOptions(tt.opts)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, uOpts)
			} else {
				require.NoError(t, err)
				require.NotNil(t, uOpts)
				assert.Equal(t, []string{tt.opts.Addr}, uOpts.Addrs)
				assert.Equal(t, tt.opts.DB, uOpts.DB)
				assert.Equal(t, tt.opts.Username, uOpts.Username)
				assert.Equal(t, tt.opts.Password, uOpts.Password)
				assert.Equal(t, tt.opts.PoolSize, uOpts.PoolSize)
			}
		})
	}
}

// isRedisAvailable checks if Redis is available for testing
func isRedisAvailable() bool {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return client.Ping(ctx).Err() == nil
}

// generateTestCert generates a self-signed certificate for testing
func generateTestCert() (certPEM, keyPEM string, err error) {
	// Generate a private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test Org"},
			Country:       []string{"US"},
			Province:      []string{"CA"},
			Locality:      []string{"Test City"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Create the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return "", "", err
	}

	// Encode certificate to PEM
	certPEMBlock := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	}
	certPEMBytes := pem.EncodeToMemory(certPEMBlock)

	// Encode private key to PEM
	keyPEMBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}
	keyPEMBytes := pem.EncodeToMemory(keyPEMBlock)

	return string(certPEMBytes), string(keyPEMBytes), nil
}
