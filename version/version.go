package version

import "os"

func Version() string {
	version, ok := os.LookupEnv("COMMIT_SHA")
	if !ok {
		version = "unknown"
	}
	if len(version) > 7 {
		version = version[:7]
	}
	return version
}
