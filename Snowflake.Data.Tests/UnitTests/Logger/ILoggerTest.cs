/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;
using Snowflake.Data.Log;
using Microsoft.Extensions.Logging;
using ILogger = Microsoft.Extensions.Logging.ILogger;
using NLog.Extensions.Logging;
using Serilog;
using Serilog.Extensions.Logging;
using System.IO;
using System;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class ILoggerTest
    {
        private const string InfoMessage = "Info message";
        private const string DebugMessage = "Debug message";
        private const string WarnMessage = "Warn message";
        private const string ErrorMessage = "Error message";
        private const string CriticalMessage = "critical message";

        private const string Log4NetFileName = "test_log4net.log";
        private const string SerilogFileName = "test_serilog.log";
        private const string NlogFileName = "test_nlog.log";

        public abstract class ILoggerBaseTest
        {
            protected ILogger _logger;
            protected string _logFile;

            [OneTimeSetUp]
            public void BeforeTest()
            {
                SnowflakeDbLoggerFactory.EnableCustomLogger();
            }

            [OneTimeTearDown]
            public void AfterTest()
            {
                // Return to default setting
                SnowflakeDbLoggerFactory.ResetCustomLogger();
                SnowflakeDbLoggerFactory.DisableCustomLogger();
                if (_logFile != null)
                {
                    File.Delete(_logFile);
                    _logFile = null;
                }
            }

            [Test]
            public void TestUsingDefaultLogger()
            {
                var originalLogger = SFLoggerFactory.GetCustomLogger<ILoggerTest>();
                SnowflakeDbLoggerFactory.ResetCustomLogger();
                _logger = SFLoggerFactory.GetCustomLogger<ILoggerTest>();
                Assert.IsInstanceOf<ILogger>(_logger);
                SnowflakeDbLoggerFactory.SetCustomLogger(originalLogger);
            }

            [Test]
            public void TestSettingCustomLogger()
            {
                var originalLogger = SFLoggerFactory.GetCustomLogger<ILoggerTest>();
                SnowflakeDbLoggerFactory.SetCustomLogger(new LoggerEmptyImpl());
                _logger = SFLoggerFactory.GetCustomLogger<ILoggerTest>();
                Assert.IsInstanceOf<LoggerEmptyImpl>(_logger);
                SnowflakeDbLoggerFactory.SetCustomLogger(originalLogger);
            }

            [Test]
            public void TestBeginScope(
                [Values(false, true)] bool isEnabled)
            {
                _logger = GetLogger(isEnabled);

                if (_logger is LoggerEmptyImpl)
                {
                    Assert.Throws<NotImplementedException>(() => _logger.BeginScope("Test"));
                }
            }

            [Test]
            public void TestIsDebugEnabled(
                [Values(false, true)] bool isEnabled)
            {
                _logger = GetLogger(isEnabled);
                Assert.AreEqual(isEnabled, _logger.IsEnabled(LogLevel.Debug));
            }

            [Test]
            public void TestIsInfoEnabled(
                [Values(false, true)] bool isEnabled)
            {
                _logger = GetLogger(isEnabled);
                Assert.AreEqual(isEnabled, _logger.IsEnabled(LogLevel.Information));
            }

            [Test]
            public void TestIsWarnEnabled(
                [Values(false, true)] bool isEnabled)
            {
                _logger = GetLogger(isEnabled);
                Assert.AreEqual(isEnabled, _logger.IsEnabled(LogLevel.Warning));
            }

            [Test]
            public void TestIsErrorEnabled(
                [Values(false, true)] bool isEnabled)
            {
                _logger = GetLogger(isEnabled);
                Assert.AreEqual(isEnabled, _logger.IsEnabled(LogLevel.Error));
            }

            [Test]
            public void TestIsFatalEnabled(
                [Values(false, true)] bool isEnabled)
            {
                _logger = GetLogger(isEnabled);
                Assert.AreEqual(isEnabled, _logger.IsEnabled(LogLevel.Critical));
            }

            private ILogger GetLogger(bool isEnabled)
            {
                if (isEnabled)
                {
                    SnowflakeDbLoggerFactory.EnableCustomLogger();
                }
                else
                {
                    SnowflakeDbLoggerFactory.DisableCustomLogger();
                }

                return SFLoggerFactory.GetCustomLogger<ILoggerTest>();
            }

            [Test]
            public void TestThatLogsToProperFileWithProperLogLevelOnly()
            {
                _logger = SFLoggerFactory.GetCustomLogger<ILoggerTest>();

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
                        Assert.IsTrue(logLines.Contains(DebugMessage));
                        Assert.IsTrue(logLines.Contains(InfoMessage));
                        Assert.IsTrue(logLines.Contains(WarnMessage));
                        Assert.IsTrue(logLines.Contains(ErrorMessage));
                        Assert.IsTrue(logLines.Contains(CriticalMessage));
                    }
                }
            }
        }

        [TestFixture]
        public class Log4NetTest : ILoggerBaseTest
        {
            [OneTimeSetUp]
            public void SetUp()
            {
                Environment.SetEnvironmentVariable("TEST_LOG4NET_FILE_NAME", Log4NetFileName);
                var factory = LoggerFactory.Create(
                    builder => builder
                    .AddLog4Net("TestLog4Net.config")
                    .SetMinimumLevel(LogLevel.Trace));

                var log4netLogger = factory.CreateLogger("Log4NetTest");
                SnowflakeDbLoggerFactory.SetCustomLogger(log4netLogger);
                _logFile = Log4NetFileName;
            }
        }

        [TestFixture]
        public class SerilogTest : ILoggerBaseTest
        {
            [OneTimeSetUp]
            public void SetUp()
            {
                var loggerSerilog = new LoggerConfiguration()
                    //.ReadFrom.Xml("TestSerilog.Config")
                    .MinimumLevel.Verbose()
                    .WriteTo.File(SerilogFileName)
                    .CreateLogger();

                var serilogLogger = new SerilogLoggerFactory(loggerSerilog).CreateLogger("SerilogTest");
                SnowflakeDbLoggerFactory.SetCustomLogger(serilogLogger);
                _logFile = SerilogFileName;
            }
        }

        [TestFixture]
        public class NlogTest : ILoggerBaseTest
        {
            [OneTimeSetUp]
            public void SetUp()
            {
                var l = SFLoggerFactory.GetLogger<ILoggerTest>();
                Environment.SetEnvironmentVariable("TEST_NLOG_FILE_NAME", NlogFileName);
                var factory = LoggerFactory.Create(
                    builder => builder
                    .AddNLog("TestNLog.config")
                    .SetMinimumLevel(LogLevel.Trace));

                var nlogLogger = factory.CreateLogger("NlogTest");
                SnowflakeDbLoggerFactory.SetCustomLogger(nlogLogger);
                _logFile = NlogFileName;
            }
        }
    }
}
