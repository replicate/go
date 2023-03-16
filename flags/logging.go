package flags

import (
	"io"

	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-server-sdk/v6/ldcomponents"
	"github.com/sirupsen/logrus"
)

func configureLogger(log logrus.FieldLogger) *ldcomponents.LoggingConfigurationBuilder {
	if log == nil {
		l := logrus.New()
		l.SetOutput(io.Discard)
		log = l
	}
	log = log.WithField("component", "launchdarkly")

	logger := ldlog.NewDefaultLoggers()
	logger.SetBaseLoggerForLevel(ldlog.Debug, &wrapLog{log.Debugln, log.Debugf})
	logger.SetBaseLoggerForLevel(ldlog.Info, &wrapLog{log.Infoln, log.Infof})
	logger.SetBaseLoggerForLevel(ldlog.Warn, &wrapLog{log.Warnln, log.Warnf})
	logger.SetBaseLoggerForLevel(ldlog.Error, &wrapLog{log.Errorln, log.Errorf})

	logging := ldcomponents.Logging().Loggers(logger)

	return logging
}

type wrapLog struct {
	println func(values ...interface{})
	printf  func(format string, values ...interface{})
}

func (l *wrapLog) Println(values ...interface{}) {
	l.println(values...)
}

func (l *wrapLog) Printf(format string, values ...interface{}) {
	l.printf(format, values...)
}
