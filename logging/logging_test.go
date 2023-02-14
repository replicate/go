package logging

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
)

func TestConfigureLevel(t *testing.T) {
	// *logrus.Logger
	log1 := logrus.New()
	// *logrus.Field
	log2 := logrus.New().WithFields(logrus.Fields{"name": "elephant"})

	defer os.Unsetenv("LOG_LEVEL")

	// Default level is INFO
	Configure(log1)
	Configure(log2)
	assert.Equal(t, logrus.InfoLevel, log1.GetLevel())
	assert.Equal(t, logrus.InfoLevel, log2.Logger.GetLevel())

	// Unparseable level => INFO
	os.Setenv("LOG_LEVEL", "garbage")
	Configure(log1)
	Configure(log2)
	assert.Equal(t, logrus.InfoLevel, log1.GetLevel())
	assert.Equal(t, logrus.InfoLevel, log2.Logger.GetLevel())

	os.Setenv("LOG_LEVEL", "warning")
	Configure(log1)
	Configure(log2)
	assert.Equal(t, logrus.WarnLevel, log1.GetLevel())
	assert.Equal(t, logrus.WarnLevel, log2.Logger.GetLevel())

	os.Setenv("LOG_LEVEL", "WARN")
	Configure(log1)
	Configure(log2)
	assert.Equal(t, logrus.WarnLevel, log1.GetLevel())
	assert.Equal(t, logrus.WarnLevel, log2.Logger.GetLevel())

	os.Setenv("LOG_LEVEL", "error")
	Configure(log1)
	Configure(log2)
	assert.Equal(t, logrus.ErrorLevel, log1.GetLevel())
	assert.Equal(t, logrus.ErrorLevel, log2.Logger.GetLevel())
}

func TestConfigureFormatter(t *testing.T) {
	// *logrus.Logger
	log1 := logrus.New()
	// *logrus.Field
	log2 := logrus.New().WithFields(logrus.Fields{"name": "elephant"})

	defer os.Unsetenv("LOG_FORMAT")

	// Default is production, i.e. JSON output
	{
		Configure(log1)
		_, ok := log1.Formatter.(*logrus.JSONFormatter)
		assert.True(t, ok)
	}
	{
		Configure(log2)
		_, ok := log2.Logger.Formatter.(*logrus.JSONFormatter)
		assert.True(t, ok)
	}

	// Unknown format => JSON output
	os.Setenv("LOG_FORMAT", "yaml")
	{
		Configure(log1)
		_, ok := log1.Formatter.(*logrus.JSONFormatter)
		assert.True(t, ok)
	}
	{
		Configure(log2)
		_, ok := log2.Logger.Formatter.(*logrus.JSONFormatter)
		assert.True(t, ok)
	}

	os.Setenv("LOG_FORMAT", "development")
	{
		Configure(log1)
		_, ok := log1.Formatter.(*logrus.TextFormatter)
		assert.True(t, ok)
	}
	{
		Configure(log2)
		_, ok := log2.Logger.Formatter.(*logrus.TextFormatter)
		assert.True(t, ok)
	}
}
