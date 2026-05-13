using System;
using System.Collections.Generic;
using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.UnitTests
{
    public sealed class SFLoggerPairTestFixture : IDisposable
    {
        public SFLoggerPairTestFixture()
        {
            // Log level defaults to Warn on net6.0 builds in github actions
            // Set the root level to Debug
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Debug, "STDOUT");
        }

        public void Dispose()
        {
            EasyLoggerManager.Instance.ResetEasyLogging(EasyLoggingLogLevel.Off);
        }
    }

    [Collection(SequentialCollection.SequentialCollectionName)]
    public class SFLoggerPairTest : IClassFixture<SFLoggerPairTestFixture>, IDisposable
    {
        private readonly SFLogger _loggerPair;
        private readonly TestAppender _testAppender;

        public SFLoggerPairTest(SFLoggerPairTestFixture fixture)
        {
            _loggerPair = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
            _testAppender = new TestAppender();
        }

        public void Dispose()
        {
            SFLoggerImpl.s_appenders.Remove(_testAppender);
        }

        [Fact]
        public void TestUsingSFLogger()
        {
            var loggerPair = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
            Assert.IsType<SFLoggerPair>(loggerPair);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsDebugEnabled(
            bool isEnabled)
        {
            var loggerPair = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.DEBUG);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, loggerPair.IsDebugEnabled());
            loggerPair.Debug("debug log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsInfoEnabled(
            bool isEnabled)
        {
            var loggerPair = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.INFO);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, loggerPair.IsInfoEnabled());
            loggerPair.Info("info log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsWarnEnabled(
            bool isEnabled)
        {
            var loggerPair = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.WARN);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, loggerPair.IsWarnEnabled());
            loggerPair.Warn("warn log message", new Exception("test exception"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestIsErrorEnabled(
            bool isEnabled)
        {
            var loggerPair = GetLogger();
            if (isEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.ERROR);
            }
            else
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF);
            }

            Assert.Equal(isEnabled, loggerPair.IsErrorEnabled());
            loggerPair.Error("error log message", new Exception("test exception"));
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
            var loggerPair = GetLogger();
            SFLoggerImpl.SetLevel(LoggingEvent.ERROR);

            // Act
            loggerPair.Error("Test error with sensitive exception", exception);

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
