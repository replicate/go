package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEd25519Signer(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}

	components := MustComponents([]string{
		`"@method"`,
		`"@target-uri"`,
		`"@authority"`,
	})
	signer, err := NewEd25519Signer(
		privateKey,
		components,
		WithLabel("default"),
		WithExpiry(5*time.Minute),
		WithKeyID("testkey123"),
	)
	require.NoError(t, err)

	req, err := http.NewRequest("GET", "https://example.com", nil)
	require.NoError(t, err)

	_, err = signer.Sign(req)
	require.NoError(t, err)

	// TODO: validate the signature!
}
