package logging

import (
	"os"

	"github.com/sirupsen/logrus"
)

func Configure(logger *logrus.Logger) {
	format := os.Getenv("LOG_FORMAT")
	if format == "development" {
		logger.SetFormatter(&logrus.TextFormatter{})
	} else {
		logger.SetFormatter(&logrus.JSONFormatter{
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyLevel: "severity",
				logrus.FieldKeyMsg:   "message",
				logrus.FieldKeyTime:  "timestamp",
			},
		})
	}

	level := os.Getenv("LOG_LEVEL")
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logger.SetLevel(logrus.InfoLevel)
	} else {
		logger.SetLevel(lvl)
	}
}
