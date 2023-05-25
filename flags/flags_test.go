package flags

import (
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/stretchr/testify/require"
)

func TestFlagDefault(t *testing.T) {
	testcontext := ldcontext.New("__test__")

	require.False(t, Flag(&testcontext, "anyflag"))
}

func TestFlagDefaultNilContext(t *testing.T) {
	var testcontext *ldcontext.Context

	// Ensure that the client is configured (albeit in offline mode)
	Init("")

	require.False(t, Flag(testcontext, "anyflag"))
}

func TestFlagSystemDefault(t *testing.T) {
	require.False(t, FlagSystem("anyflag"))
}

func TestKillSwitchDefault(t *testing.T) {
	testcontext := ldcontext.New("__test__")

	require.True(t, KillSwitch(&testcontext, "anyflag"))
}

func TestKillSwitchSystemDefault(t *testing.T) {
	require.True(t, KillSwitchSystem("anyflag"))
}
