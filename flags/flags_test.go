package flags

import (
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/stretchr/testify/assert"
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

func TestFlagBlueYellowDefault(t *testing.T) {
	testcontext := ldcontext.New("__test__")

	assert.Equal(t, FlagBlueYellow(&testcontext, "anyflag", ResultBlue), ResultBlue)
	assert.Equal(t, FlagBlueYellow(&testcontext, "anyflag", ResultYellow), ResultYellow)
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

func TestOverrides(t *testing.T) {
	testcontext := ldcontext.New("__test__")

	Override(func(o map[string]bool) {
		o["myflag"] = true
		o["otherflag"] = false
	})

	require.True(t, Flag(&testcontext, "myflag"))
	require.False(t, KillSwitch(&testcontext, "otherflag"))

	ClearOverrides()

	require.False(t, Flag(&testcontext, "myflag"))
	require.True(t, KillSwitch(&testcontext, "otherflag"))
}
