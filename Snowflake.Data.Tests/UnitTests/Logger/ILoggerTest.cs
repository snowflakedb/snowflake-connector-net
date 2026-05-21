using Xunit;
using Snowflake.Data.Log;
using Microsoft.Extensions.Logging;
using ILogger = Microsoft.Extensions.Logging.ILogger;
using System.IO;
using System;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    abstract class ILoggerTest
    {
        private const string InfoMessage = "Info message";
        private const string DebugMessage = "Debug message";
        private const string WarnMessage = "Warn message";
        private const string ErrorMessage = "Error message";
        private const string CriticalMessage = "critical message";

        protected ILogger _customLogger;
        protected ILogger _logger;
        protected string _logFile;

        [OneTimeTearDown]
        public void AfterTest()
        {
            // Return to default setting
            SnowflakeDbLoggerConfig.ResetCustomLogger();
            if (_logFile != null)
            {
                File.Delete(_logFile);
                _logFile = null;
            }
        }

        [SFFact]
        public void TestResetCustomLogger()
        {
            SnowflakeDbLoggerConfig.ResetCustomLogger();
            Assert.InstanceOf<ILogger>(SFLoggerFactory.s_customLogger);
        }

        [SFFact]
        public void TestSettingCustomLogger()
        {
            SnowflakeDbLoggerConfig.SetCustomLogger(new LoggerEmptyImpl());
            Assert.InstanceOf<LoggerEmptyImpl>(SFLoggerFactory.s_customLogger);
        }

        [SFFact]
        public void TestBeginScope(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            if (_logger is LoggerEmptyImpl)
            {
                Assert.Throws<NotImplementedException>(() => _logger.BeginScope("Test"));
            }
        }

        [SFFact]
        public void TestIsDebugEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            Assert.Equal(isEnabled, _logger.IsEnabled(LogLevel.Debug));
        }

        [SFFact]
        public void TestIsInfoEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            Assert.Equal(isEnabled, _logger.IsEnabled(LogLevel.Information));
        }

        [SFFact]
        public void TestIsWarnEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            Assert.Equal(isEnabled, _logger.IsEnabled(LogLevel.Warning));
        }

        [SFFact]
        public void TestIsErrorEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            Assert.Equal(isEnabled, _logger.IsEnabled(LogLevel.Error));
        }

        [SFFact]
        public void TestIsFatalEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            Assert.Equal(isEnabled, _logger.IsEnabled(LogLevel.Critical));
        }

        private ILogger GetLogger(bool isEnabled)
        {
            if (isEnabled)
            {
                SnowflakeDbLoggerConfig.SetCustomLogger(_customLogger);
            }
            else
            {
                SnowflakeDbLoggerConfig.ResetCustomLogger();
            }

            return SFLoggerFactory.s_customLogger;
        }

        [SFFact]
        public void TestThatLogsToProperFileWithProperLogLevelOnly()
        {
            _logger = GetLogger(true);

            // act
            _logger.LogDebug(DebugMessage);
            _logger.LogInformation(InfoMessage);
            _logger.LogWarning(WarnMessage);
            _logger.LogError(ErrorMessage);
            _logger.LogCritical(CriticalMessage);

            // assert
            using (FileStream logFileStream = new FileStream(_logFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                using (StreamReader logFileReader = new StreamReader(logFileStream))
                {
                    string logLines = logFileReader.ReadToEnd();
                    Assert.True(logLines.Contains(DebugMessage));
                    Assert.True(logLines.Contains(InfoMessage));
                    Assert.True(logLines.Contains(WarnMessage));
                    Assert.True(logLines.Contains(ErrorMessage));
                    Assert.True(logLines.Contains(CriticalMessage));
                }
            }
        }
    }
}
