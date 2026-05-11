using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;
using System;
using System.Collections.Generic;

namespace Snowflake.Data.Tests.UnitTests
{
    class SFLoggerPairTest
    {
        SFLogger _loggerPair;
        TestAppender _testAppender;
        public static void BeforeAll()
        {
            // Log level defaults to Warn on net6.0 builds in github actions
            // Set the root level to Debug
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Debug, "STDOUT");
        }
        public static void AfterAll()
        {
            EasyLoggerManager.Instance.ResetEasyLogging(EasyLoggingLogLevel.Off);
        }
        public void BeforeTest()
        {
            _loggerPair = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
            _testAppender = new TestAppender();
        }
        public void AfterTest()
        {
            SFLoggerImpl.s_appenders.Remove(_testAppender);
        }

        [Fact]
        public void TestUsingSFLogger()
        {
            _loggerPair = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
            Assert.IsType<SFLoggerPair>(_loggerPair);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsDebugEnabled(
            bool isEnabled)
        {
            _loggerPair = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.DEBUG);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _loggerPair.IsDebugEnabled());
            _loggerPair.Debug("debug log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsInfoEnabled(
            bool isEnabled)
        {
            _loggerPair = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.INFO);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _loggerPair.IsInfoEnabled());
            _loggerPair.Info("info log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsWarnEnabled(
            bool isEnabled)
        {
            _loggerPair = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.WARN);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _loggerPair.IsWarnEnabled());
            _loggerPair.Warn("warn log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsErrorEnabled(
            bool isEnabled)
        {
            _loggerPair = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.ERROR);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, _loggerPair.IsErrorEnabled());
            _loggerPair.Error("error log message", new Exception("test exception"));
        }

        private SFLogger GetLogger()
        {
            var logger = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
            EasyLoggerManager.AddConsoleAppender();
            return logger;
        }

        [Fact]
        public void TestMaskedExceptionWithSensitiveData()
        {
            // Arrange
            SFLoggerImpl.s_appenders.Add(_testAppender);
            var sensitiveMessage = "Connection failed with password='MySecretPass123'";
            var exception = new Exception(sensitiveMessage);
            _loggerPair = GetLogger();
            SFLoggerImpl.SetLevel(LoggingEvent.ERROR);

            // Act
            _loggerPair.Error("Test error with sensitive exception", exception);

            // Assert
            var loggedExceptionString = _testAppender.LoggedExceptions[0]?.ToString() ?? "";
            var expectedMaskedString = "System.Exception: Connection failed with password=****";
            Assert.Equal(expectedMaskedString, loggedExceptionString);
        }

        private class TestAppender : SFAppender
        {
            public List<string> LoggedMessages = new();
            public List<Exception> LoggedExceptions = new();

            public void Append(string logLevel, string message, Type type, Exception ex = null)
            {
                LoggedMessages.Add(message);
                LoggedExceptions.Add(ex);
            }

            public void ActivateOptions()
            {
                // No-op for testing
            }
        }
    }
}
