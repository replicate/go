package logging

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

// New creates a new logger with a default "logger" field so we can identify the
// source of log messages.
func New(name string) *logrus.Entry {
	log := logrus.New()

	Configure(log)

	return log.WithFields(logrus.Fields{
		"logger": name,
	})
}

func Configure(logger logrus.FieldLogger) {
	var formatter logrus.Formatter
	var level logrus.Level

	format := os.Getenv("LOG_FORMAT")
	if format == "development" {
		formatter = &logrus.TextFormatter{}
	} else {
		formatter = &logrus.JSONFormatter{
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyLevel: "severity",
				logrus.FieldKeyMsg:   "message",
				logrus.FieldKeyTime:  "timestamp",
			},
		}
	}

	lvl, err := logrus.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		level = logrus.InfoLevel
	} else {
		level = lvl
	}

	switch l := logger.(type) {
	case *logrus.Logger:
		l.SetFormatter(formatter)
		l.SetLevel(level)
	case *logrus.Entry:
		l.Logger.SetFormatter(formatter)
		l.Logger.SetLevel(level)
	default:
		panic(fmt.Sprintf("don't know how to configure logger %v", l))
	}
}
