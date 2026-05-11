using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;
using System;

namespace Snowflake.Data.Tests.UnitTests
{
    class SFLoggerTest
    {
        private SFLogger _logger;
        public static void BeforeTest()
        {
            // Log level defaults to Warn on net6.0 builds in github actions
            // Set the root level to Debug
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Debug, "STDOUT");
        }
        public static void AfterAll()
        {
            EasyLoggerManager.Instance.ResetEasyLogging(EasyLoggingLogLevel.Off);
        }

        [Fact]
        public void TestUsingSFLogger()
        {
            _logger = SFLoggerFactory.GetSFLogger<SFLoggerTest>();
            Assert.IsType<SFLoggerImpl>(_logger);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsDebugEnabled(
            bool isEnabled)
        {
            _logger = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.DEBUG);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _logger.IsDebugEnabled());
            _logger.Debug("debug log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsInfoEnabled(
            bool isEnabled)
        {
            _logger = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.INFO);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _logger.IsInfoEnabled());
            _logger.Info("info log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsWarnEnabled(
            bool isEnabled)
        {
            _logger = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.WARN);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _logger.IsWarnEnabled());
            _logger.Warn("warn log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsErrorEnabled(
            bool isEnabled)
        {
            _logger = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.ERROR);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _logger.IsErrorEnabled());
            _logger.Error("error log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false, LoggingEvent.OFF)]
        [InlineData(false, LoggingEvent.TRACE)]
        [InlineData(false, LoggingEvent.DEBUG)]
        [InlineData(false, LoggingEvent.INFO)]
        [InlineData(false, LoggingEvent.WARN)]
        [InlineData(false, LoggingEvent.ERROR)]
        [InlineData(true, LoggingEvent.OFF)]
        [InlineData(true, LoggingEvent.TRACE)]
        [InlineData(true, LoggingEvent.DEBUG)]
        [InlineData(true, LoggingEvent.INFO)]
        [InlineData(true, LoggingEvent.WARN)]
        [InlineData(true, LoggingEvent.ERROR)]
        public void TestSetLevel(
            bool isEnabled,
            LoggingEvent logLevel)
        {
            _logger = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(logLevel);
                Assert.Equal(logLevel, SFLoggerImpl.s_level);

                if (logLevel == LoggingEvent.OFF)
                {
                    Assert.False(_logger.IsDebugEnabled());
                    Assert.False(_logger.IsInfoEnabled());
                    Assert.False(_logger.IsWarnEnabled());
                    Assert.False(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.TRACE)
                {
                    Assert.True(_logger.IsDebugEnabled());
                    Assert.True(_logger.IsInfoEnabled());
                    Assert.True(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.DEBUG)
                {
                    Assert.True(_logger.IsDebugEnabled());
                    Assert.True(_logger.IsInfoEnabled());
                    Assert.True(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.INFO)
                {
                    Assert.False(_logger.IsDebugEnabled());
                    Assert.True(_logger.IsInfoEnabled());
                    Assert.True(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.WARN)
                {
                    Assert.False(_logger.IsDebugEnabled());
                    Assert.False(_logger.IsInfoEnabled());
                    Assert.True(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.ERROR)
                {
                    Assert.False(_logger.IsDebugEnabled());
                    Assert.False(_logger.IsInfoEnabled());
                    Assert.False(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
            }
        }

        private SFLogger GetLogger()
        {
            var logger = SFLoggerFactory.GetSFLogger<SFLoggerTest>();
            return logger;
        }
    }
}
