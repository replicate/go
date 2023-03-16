package flags

import "github.com/launchdarkly/go-sdk-common/v3/ldcontext"

const versionKind = "version"

func GetVersion(id string) ldcontext.Context {
	// We rely on web to have populated other fields (hardware, model.owner,
	// etc.) in LaunchDarkly.
	return ldcontext.NewWithKind(versionKind, id)
}
