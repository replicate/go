package flags

import (
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	ld "github.com/launchdarkly/go-server-sdk/v6"

	"github.com/replicate/go/logging"
)

var currentClient *ld.LDClient
var logger = logging.New("flags")

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
	return lookupDefault(context, name, false, currentClient.BoolVariation)
}

func FlagSystem(name string) bool {
	return lookupDefault(&systemUser, name, false, currentClient.BoolVariation)
}

func KillSwitch(context *ldcontext.Context, name string) bool {
	return lookupDefault(context, name, true, currentClient.BoolVariation)
}

func KillSwitchSystem(name string) bool {
	return lookupDefault(&systemUser, name, true, currentClient.BoolVariation)
}

func Int(context *ldcontext.Context, name string) int {
	return lookupDefault(context, name, 0, currentClient.IntVariation)
}

func Float64(context *ldcontext.Context, name string) float64 {
	return lookupDefault(context, name, 0.0, currentClient.Float64Variation)
}

func String(context *ldcontext.Context, name string) string {
	return lookupDefault(context, name, "", currentClient.StringVariation)
}

func lookupDefault[T any](context *ldcontext.Context, name string, defaultVal T, variationFunc func(string, ldcontext.Context, T) (T, error)) T {
	log := logger.Sugar()

	if currentClient == nil {
		return defaultVal
	}
	if context == nil {
		log.Warnw("flags package was passed a nil context: returning default value", "flag", name, "default_value", defaultVal)
		return defaultVal
	}
	// Variation functions only return an error in the event that flags are
	// not available (e.g. if LaunchDarkly is having an outage or we've
	// misconfigured the client).
	result, err := variationFunc(name, *context, defaultVal)
	if err != nil {
		log.Warnf("Failed to fetch value for flag '%s' (returning default %v to caller): %v", name, defaultVal, err)
	}
	return result
}
