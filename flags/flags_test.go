package flags

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/launchdarkly/go-sdk-common.v2/lduser"
)

func TestFlagDefault(t *testing.T) {
	testuser := lduser.NewUserBuilder("__test__").Build()

	require.False(t, Flag(&testuser, "anyflag"))
}

func TestFlagSystemDefault(t *testing.T) {
	require.False(t, FlagSystem("anyflag"))
}

func TestKillSwitchDefault(t *testing.T) {
	testuser := lduser.NewUserBuilder("__test__").Build()

	require.True(t, KillSwitch(&testuser, "anyflag"))
}

func TestKillSwitchSystemDefault(t *testing.T) {
	require.True(t, KillSwitchSystem("anyflag"))
}
