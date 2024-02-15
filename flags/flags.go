package flags

import (
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	ld "github.com/launchdarkly/go-server-sdk/v6"

	"github.com/replicate/go/logging"
)

var currentClient *ld.LDClient
var logger = logging.New("flags")

var overrides = make(map[string]bool)

type BlueYellowResult string

const (
	ResultBlue   BlueYellowResult = "blue"
	ResultYellow BlueYellowResult = "yellow"
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

// Override allows setting flag overrides. This is usually only used in the
// context of testing.
func Override(f func(map[string]bool)) {
	f(overrides)
}

func ClearOverrides() {
	overrides = make(map[string]bool)
}

// FlagBlueYellow is a wrapper around a boolean flag that serves to
// conventionalise a blue/green (or in this case blue/yellow) rollout pattern.
//
// You would use it in code that needs to roll out a behavior in both directions
// -- from yellow to blue and from blue to yellow -- but with a safe fallback
// value (the provided default) once a rollout is complete. This guards against
// the possibility of a LaunchDarkly outage accidentally reverting a rollout.
//
// While the naming is idiosyncratic, it serves to emphasise that this is not a
// "normal" blue-green flag, and care is required when setting up the flag in
// LaunchDarkly.
//
// You should label the LaunchDarkly variations clearly. The true variation
// should be "Blue", and the false variation "Yellow". The names should match
// the default colors assigned to boolean flag variations in LaunchDarkly.
//
// Code that uses this should look something like the following:
//
//	rolloutDefault := flags.ResultBlue
//	if flags.FlagBlueYellow(&flagContext, "my-rollout-flag", rolloutDefault) == flags.ResultBlue {
//	  // behavior when blue...
//	} else {
//	  // behavior when yellow...
//	}
//
// Depending on the situation, you would change the default in code (here,
// `rolloutDefault`) either when starting a new rollout, or when one has
// completed. Note: the default value will *only* be used if either
//
// - LaunchDarkly cannot be contacted
// - the flag is not defined in LaunchDarkly
//
// A LaunchDarkly flag used for blue-yellow rollouts should be clearly
// annotated, and usually left with targeting switched on at all times.
func FlagBlueYellow(context *ldcontext.Context, name string, defaultVal BlueYellowResult) BlueYellowResult {
	if lookupDefault(context, name, defaultVal == ResultBlue) {
		return ResultBlue
	}
	return ResultYellow
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

	if result, ok := overrides[name]; ok {
		return result
	}
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
