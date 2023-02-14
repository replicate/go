package logging

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
)

func TestConfigureLevel(t *testing.T) {
	log := logrus.New()

	defer os.Unsetenv("LOG_LEVEL")

	// Default level is INFO
	Configure(log)
	assert.Equal(t, logrus.InfoLevel, log.GetLevel())

	// Unparseable level => INFO
	os.Setenv("LOG_LEVEL", "garbage")
	Configure(log)
	assert.Equal(t, logrus.InfoLevel, log.GetLevel())

	os.Setenv("LOG_LEVEL", "warning")
	Configure(log)
	assert.Equal(t, logrus.WarnLevel, log.GetLevel())

	os.Setenv("LOG_LEVEL", "WARN")
	Configure(log)
	assert.Equal(t, logrus.WarnLevel, log.GetLevel())

	os.Setenv("LOG_LEVEL", "error")
	Configure(log)
	assert.Equal(t, logrus.ErrorLevel, log.GetLevel())
}

func TestConfigureFormatter(t *testing.T) {
	log := logrus.New()

	defer os.Unsetenv("LOG_FORMAT")

	// Default is production, i.e. JSON output
	{
		Configure(log)
		_, ok := log.Formatter.(*logrus.JSONFormatter)
		assert.True(t, ok)
	}

	// Unknown format => JSON output
	os.Setenv("LOG_FORMAT", "yaml")
	{
		Configure(log)
		_, ok := log.Formatter.(*logrus.JSONFormatter)
		assert.True(t, ok)
	}

	os.Setenv("LOG_FORMAT", "development")
	{
		Configure(log)
		_, ok := log.Formatter.(*logrus.TextFormatter)
		assert.True(t, ok)
	}
}
