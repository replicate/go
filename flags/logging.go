package flags

import (
	"io"

	"github.com/sirupsen/logrus"
	"gopkg.in/launchdarkly/go-sdk-common.v2/ldlog"
	"gopkg.in/launchdarkly/go-server-sdk.v5/interfaces"
	"gopkg.in/launchdarkly/go-server-sdk.v5/ldcomponents"
)

func configureLogger(log logrus.FieldLogger) interfaces.LoggingConfigurationFactory {
	if log == nil {
		l := logrus.New()
		l.SetOutput(io.Discard)
		log = l
	}
	log = log.WithField("component", "launchdarkly")

	return &logCreator{log: log}
}

type logCreator struct {
	log logrus.FieldLogger
}

func (c *logCreator) CreateLoggingConfiguration(b interfaces.BasicConfiguration) (interfaces.LoggingConfiguration, error) { // nolint:gocritic
	logger := ldlog.NewDefaultLoggers()
	logger.SetBaseLoggerForLevel(ldlog.Debug, &wrapLog{c.log.Debugln, c.log.Debugf})
	logger.SetBaseLoggerForLevel(ldlog.Info, &wrapLog{c.log.Infoln, c.log.Infof})
	logger.SetBaseLoggerForLevel(ldlog.Warn, &wrapLog{c.log.Warnln, c.log.Warnf})
	logger.SetBaseLoggerForLevel(ldlog.Error, &wrapLog{c.log.Errorln, c.log.Errorf})
	return ldcomponents.Logging().Loggers(logger).CreateLoggingConfiguration(b)
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
