package flags

import (
	"context"
	"net/http"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/stretchr/testify/require"
)

// This mostly serves as a quick check that web and api are generating the same
// keys for the same user IDs.
func TestGetUser(t *testing.T) {
	r, _ := http.NewRequest("GET", "https://example.com", nil)

	require.Equal(t, GetUser(1, r).Key(), "e7")
	require.Equal(t, GetUser(12345, r).Key(), "e05Y")
	require.Equal(t, GetUser(36395, r).Key(), "J3qj")
}

func TestGetUserZeroID(t *testing.T) {
	r, _ := http.NewRequest("GET", "https://example.com", nil)

	require.Equal(t, GetUser(0, r).Key(), "__unknown__")
}

func TestGetUserIP(t *testing.T) {
	r, _ := http.NewRequest("GET", "https://example.com", nil)
	r.Header.Set("CF-Connecting-IP", "203.0.113.24")

	require.Equal(t, GetUser(12345, r).GetValue("ip").StringValue(), "203.0.113.24")
}

func TestWithFlagContextStoresValueOnGoContext(t *testing.T) {
	testUser := ldcontext.NewBuilder("giraffe").Build()
	ctx := WithFlagContext(context.Background(), testUser)
	retrievedUser := FlagContextFromContext(ctx)

	require.Equal(t, testUser, retrievedUser)
}

func TestFlagContextFromContextRetrievesUnknownUserIfNothingSaved(t *testing.T) {
	bg := context.Background()
	retrievedUser := FlagContextFromContext(bg)

	require.Equal(t, unknownUser, retrievedUser)
}
