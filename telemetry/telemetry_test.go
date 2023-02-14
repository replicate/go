package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStart is the most basic of smoke tests to ensure that we can at least
// instantiate and start the telemetry package.
func TestStart(t *testing.T) {
	ctx := context.Background()

	tel, err := Start(ctx)

	require.NoError(t, err)

	require.NoError(t, tel.Shutdown(ctx))
}
