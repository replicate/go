package logging

import (
	"os"
	"testing"

	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
)

func TestNewConfigLevel(t *testing.T) {
	defer os.Unsetenv("LOG_LEVEL")

	// Default level is INFO
	{
		config := NewConfig()
		assert.Equal(t, zap.InfoLevel, config.Level.Level())
	}

	// Unparseable level => INFO
	os.Setenv("LOG_LEVEL", "garbage")
	{
		config := NewConfig()
		assert.Equal(t, zap.InfoLevel, config.Level.Level())
	}

	os.Setenv("LOG_LEVEL", "warning")
	{
		config := NewConfig()
		assert.Equal(t, zap.WarnLevel, config.Level.Level())
	}

	os.Setenv("LOG_LEVEL", "WARN")
	{
		config := NewConfig()
		assert.Equal(t, zap.WarnLevel, config.Level.Level())
	}

	os.Setenv("LOG_LEVEL", "error")
	{
		config := NewConfig()
		assert.Equal(t, zap.ErrorLevel, config.Level.Level())
	}
}

func TestNewConfigFormat(t *testing.T) {
	defer os.Unsetenv("LOG_FORMAT")

	// Default is production, i.e. JSON output
	{
		config := NewConfig()
		assert.Equal(t, "json", config.Encoding)
	}

	// Unknown format => JSON output
	os.Setenv("LOG_FORMAT", "yaml")
	{
		config := NewConfig()
		assert.Equal(t, "json", config.Encoding)
	}

	os.Setenv("LOG_FORMAT", "development")
	{
		config := NewConfig()
		assert.Equal(t, "console", config.Encoding)
	}
}
