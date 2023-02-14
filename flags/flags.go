package flags

import (
	"time"

	"github.com/replicate/go/logging"
	"gopkg.in/launchdarkly/go-sdk-common.v2/lduser"
	ld "gopkg.in/launchdarkly/go-server-sdk.v5"
)

var currentClient *ld.LDClient
var log = logging.New("flags")

func init() {
	logging.Configure(log)
}

func Init(key string) {
	config := ld.Config{
		Logging: configureLogger(log),
	}

	if key == "" {
		config.Offline = true
	}

	client, err := ld.MakeCustomClient(key, config, 5*time.Second)
	if err != nil {
		log.WithError(err).Warn("failed to make LaunchDarkly client")
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

func Flag(user *lduser.User, name string) bool {
	return lookupDefault(user, name, false)
}

func FlagSystem(name string) bool {
	return lookupDefault(&systemUser, name, false)
}

func KillSwitch(user *lduser.User, name string) bool {
	return lookupDefault(user, name, true)
}

func KillSwitchSystem(name string) bool {
	return lookupDefault(&systemUser, name, true)
}

func lookupDefault(user *lduser.User, name string, defaultVal bool) bool {
	if currentClient == nil {
		return defaultVal
	}
	// BoolVariation and friends only return an error in the event that flags are
	// not available (e.g. if LaunchDarkly is having an outage or we've
	// misconfigured the client).
	result, err := currentClient.BoolVariation(name, *user, defaultVal)
	if err != nil {
		log.Warnf("Failed to fetch value for flag '%s' (returning default %v to caller): %v", name, defaultVal, err)
	}
	return result
}
