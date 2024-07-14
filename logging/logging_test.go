package logging

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/stretchr/testify/assert"
)

func TestNewConfigLevel(t *testing.T) {
	testcases := []struct {
		value string
		level zapcore.Level
	}{
		{
			value: "",
			level: zap.InfoLevel,
		},
		{
			value: "garbage",
			level: zap.InfoLevel,
		},
		{
			value: "warning",
			level: zap.WarnLevel,
		},
		{
			value: "WARN",
			level: zap.WarnLevel,
		},
		{
			value: "error",
			level: zap.ErrorLevel,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("LOG_FORMAT", "")
			t.Setenv("LOG_LEVEL", tc.value)
			config := NewConfig()
			assert.Equal(t, tc.level, config.Level.Level())
		})
	}
}

func TestNewConfigFormat(t *testing.T) {
	testcases := []struct {
		value    string
		encoding string
	}{
		{
			value:    "",
			encoding: "json",
		},
		{
			value:    "yaml", // Unknown format
			encoding: "json",
		},
		{
			value:    "development",
			encoding: "console",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("LOG_FORMAT", tc.value)
			config := NewConfig()
			assert.Equal(t, tc.encoding, config.Encoding)
		})
	}
}
