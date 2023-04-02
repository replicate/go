package flags

import (
	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-server-sdk/v6/ldcomponents"
	"go.uber.org/zap"
)

func configureLogger(log *zap.Logger) *ldcomponents.LoggingConfigurationBuilder {
	if log == nil {
		log = zap.NewNop()
	}
	log = log.With(zap.String("component", "launchdarkly"))

	l := log.Sugar()
	logger := ldlog.NewDefaultLoggers()
	logger.SetBaseLoggerForLevel(ldlog.Debug, &wrapLog{l.Debugln, l.Debugf})
	logger.SetBaseLoggerForLevel(ldlog.Info, &wrapLog{l.Infoln, l.Infof})
	logger.SetBaseLoggerForLevel(ldlog.Warn, &wrapLog{l.Warnln, l.Warnf})
	logger.SetBaseLoggerForLevel(ldlog.Error, &wrapLog{l.Errorln, l.Errorf})

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
