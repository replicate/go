package flags

import (
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	ld "github.com/launchdarkly/go-server-sdk/v6"

	"github.com/replicate/go/logging"
)

var currentClient *ld.LDClient
var logger = logging.New("flags")

type BlueGreenResult string

const (
	ResultBlue  BlueGreenResult = "blue"
	ResultGreen BlueGreenResult = "green"
)

func Init(key string) {
	log := logger.Sugar()

	config := ld.Config{
		Logging: configureLogger(logger),
	}

	if key == "" {
		config.Offline = true
	}

	client, err := ld.MakeCustomClient(key, config, 5*time.Second)
	if err != nil {
		log.Warnw("failed to make LaunchDarkly client", "error", err)
	}

	if !client.Initialized() {
		log.Warn("failed to initialize LaunchDarkly client")
	}

	currentClient = client
}

func Close() error {
	if currentClient == nil {
		return nil
	}
	return currentClient.Close()
}

func Flag(context *ldcontext.Context, name string) bool {
	return lookupDefault(context, name, false)
}

// FlagBlueGreen is a wrapper around a boolean flag that serves to
// conventionalise a blue/green rollout pattern.
//
// You would use it in code that needs to roll out a behavior in both directions
// -- from green to blue and from blue to green -- but with a safe fallback
// value (the provided default) once a rollout is complete. This guards against
// the possibility of a LaunchDarkly outage reverting a blue-green rollout.
//
// It is highly recommended that you label the LaunchDarkly variations clearly.
// The true variation should be "Blue", and the false variation "Green".
func FlagBlueGreen(context *ldcontext.Context, name string, defaultVal BlueGreenResult) BlueGreenResult {
	if lookupDefault(context, name, defaultVal == ResultBlue) {
		return ResultBlue
	}
	return ResultGreen
}

func FlagSystem(name string) bool {
	return lookupDefault(&systemUser, name, false)
}

func KillSwitch(context *ldcontext.Context, name string) bool {
	return lookupDefault(context, name, true)
}

func KillSwitchSystem(name string) bool {
	return lookupDefault(&systemUser, name, true)
}

func lookupDefault(context *ldcontext.Context, name string, defaultVal bool) bool {
	log := logger.Sugar()

	if currentClient == nil {
		return defaultVal
	}
	if context == nil {
		log.Warnw("flags package was passed a nil context: returning default value", "flag", name, "default_value", defaultVal)
		return defaultVal
	}
	// BoolVariation and friends only return an error in the event that flags are
	// not available (e.g. if LaunchDarkly is having an outage or we've
	// misconfigured the client).
	result, err := currentClient.BoolVariation(name, *context, defaultVal)
	if err != nil {
		log.Warnf("Failed to fetch value for flag '%s' (returning default %v to caller): %v", name, defaultVal, err)
	}
	return result
}
