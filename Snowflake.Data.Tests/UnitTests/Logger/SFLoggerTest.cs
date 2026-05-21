using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;
using System;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class SFLoggerTest
    {
        private SFLogger _logger;

        [OneTimeSetUp]
        public static void BeforeTest()
        {
            // Log level defaults to Warn on net6.0 builds in github actions
            // Set the root level to Debug
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Debug, "STDOUT");
        }

        [OneTimeTearDown]
        public static void AfterAll()
        {
            EasyLoggerManager.Instance.ResetEasyLogging(EasyLoggingLogLevel.Off);
        }

        [Test]
        public void TestUsingSFLogger()
        {
            _logger = SFLoggerFactory.GetSFLogger<SFLoggerTest>();
            Assert.InstanceOf<SFLoggerImpl>(_logger);
        }

        [Test]
        public void TestIsTraceEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.TRACE);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _logger.IsTraceEnabled());
            _logger.Trace("trace log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsDebugEnabled(
            [Values(false, true)] bool isEnabled)
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

        [Test]
        public void TestIsInfoEnabled(
            [Values(false, true)] bool isEnabled)
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

        [Test]
        public void TestIsWarnEnabled(
            [Values(false, true)] bool isEnabled)
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

        [Test]
        public void TestIsErrorEnabled(
            [Values(false, true)] bool isEnabled)
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

        [Test]
        public void TestSetLevel(
            [Values(false, true)] bool isEnabled,
            [Values] LoggingEvent logLevel)
        {
            _logger = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(logLevel);
                Assert.Equal(logLevel, SFLoggerImpl.s_level);

                if (logLevel == LoggingEvent.OFF)
                {
                    Assert.False(_logger.IsTraceEnabled());
                    Assert.False(_logger.IsDebugEnabled());
                    Assert.False(_logger.IsInfoEnabled());
                    Assert.False(_logger.IsWarnEnabled());
                    Assert.False(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.TRACE)
                {
                    Assert.True(_logger.IsTraceEnabled());
                    Assert.True(_logger.IsDebugEnabled());
                    Assert.True(_logger.IsInfoEnabled());
                    Assert.True(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.DEBUG)
                {
                    Assert.False(_logger.IsTraceEnabled());
                    Assert.True(_logger.IsDebugEnabled());
                    Assert.True(_logger.IsInfoEnabled());
                    Assert.True(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.INFO)
                {
                    Assert.False(_logger.IsTraceEnabled());
                    Assert.False(_logger.IsDebugEnabled());
                    Assert.True(_logger.IsInfoEnabled());
                    Assert.True(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.WARN)
                {
                    Assert.False(_logger.IsTraceEnabled());
                    Assert.False(_logger.IsDebugEnabled());
                    Assert.False(_logger.IsInfoEnabled());
                    Assert.True(_logger.IsWarnEnabled());
                    Assert.True(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.ERROR)
                {
                    Assert.False(_logger.IsTraceEnabled());
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
