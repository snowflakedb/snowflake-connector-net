using System;
using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.UnitTests
{
    public sealed class SFLoggerTestFixture : IDisposable
    {
        public SFLoggerTestFixture()
        {
        }

        public void Dispose()
        {
            EasyLoggerManager.Instance.ResetEasyLogging(EasyLoggingLogLevel.Off);
        }
    }

    public class SFLoggerTest : IClassFixture<SFLoggerTestFixture>
    {
        private readonly SFLogger _logger;

        public SFLoggerTest(SFLoggerTestFixture fixture)
        {
            // Per-test setup: reconfigure easy logging for each test
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Debug, "STDOUT");
            _logger = SFLoggerFactory.GetSFLogger<SFLoggerTest>();
        }

        [Fact]
        public void TestUsingSFLogger()
        {
            Assert.IsType<SFLoggerImpl>(_logger);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsDebugEnabled(
            bool isEnabled)
        {
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
        [InlineData(false, 0)] // OFF
        [InlineData(false, 1)] // TRACE
        [InlineData(false, 2)] // DEBUG
        [InlineData(false, 3)] // INFO
        [InlineData(false, 4)] // WARN
        [InlineData(false, 5)] // ERROR
        [InlineData(true, 0)] // OFF
        [InlineData(true, 1)] // TRACE
        [InlineData(true, 2)] // DEBUG
        [InlineData(true, 3)] // INFO
        [InlineData(true, 4)] // WARN
        [InlineData(true, 5)] // ERROR
        public void TestSetLevel(
            bool isEnabled,
            int logLevelInt)
        {
            var logLevel = (LoggingEvent)logLevelInt;
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
    }
}
