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

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class SFLoggerTest
    {
        private const string InfoMessage = "Info message";
        private const string DebugMessage = "Debug message";
        private const string WarnMessage = "Warn message";
        private const string ErrorMessage = "Error message";
        private const string CriticalMessage = "critical message";

        public abstract class SFBaseLoggerTest
        {
            protected ILogger _logger;
            protected string _logFile;

            [OneTimeSetUp]
            public void BeforeTest()
            {
                SFLoggerFactory.EnableLogger();
            }

            [OneTimeTearDown]
            public void AfterTest()
            {
                // Return to default setting
                SFLoggerFactory.UseDefaultLogger();
                SFLoggerFactory.DisableLogger();
                if (_logFile != null)
                {
                    File.Delete(_logFile);
                    _logFile = null;
                }
            }

            [Test]
            public void TestUsingDefaultLogger()
            {
                var originalLogger = SFLoggerFactory.GetLogger<SFLoggerTest>();
                SFLoggerFactory.UseDefaultLogger();
                _logger = SFLoggerFactory.GetLogger<SFLoggerTest>();
                Assert.IsInstanceOf<ILogger>(_logger);
                SFLoggerFactory.SetCustomLogger(originalLogger);
            }

            [Test]
            public void TestSettingCustomLogger()
            {
                var originalLogger = SFLoggerFactory.GetLogger<SFLoggerTest>();
                SFLoggerFactory.SetCustomLogger(new SFLoggerEmptyImpl());
                _logger = SFLoggerFactory.GetLogger<SFLoggerTest>();
                Assert.IsInstanceOf<SFLoggerEmptyImpl>(_logger);
                SFLoggerFactory.SetCustomLogger(originalLogger);
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
                    SFLoggerFactory.EnableLogger();
                }
                else
                {
                    SFLoggerFactory.DisableLogger();
                }

                return SFLoggerFactory.GetLogger<SFLoggerTest>();
            }

            [Test]
            public void TestThatLogsToProperFileWithProperLogLevelOnly()
            {
                _logger = SFLoggerFactory.GetLogger<SFLoggerTest>();

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
        public class Log4NetTest : SFBaseLoggerTest
        {
            [OneTimeSetUp]
            public void SetUp()
            {
                var factory = LoggerFactory.Create(
                    builder => builder
                    .AddLog4Net("TestLog4Net.config")
                    .SetMinimumLevel(LogLevel.Trace));

                var log4netLogger = factory.CreateLogger("Log4NetTest");
                SFLoggerFactory.SetCustomLogger(log4netLogger);
                _logFile = "test_log4net.log";
            }
        }

        [TestFixture]
        public class SerilogTest : SFBaseLoggerTest
        {
            [OneTimeSetUp]
            public void SetUp()
            {
                var loggerSerilog = new LoggerConfiguration()
                    .MinimumLevel.Verbose()
                    .WriteTo.File("test_serilog.log")
                    .CreateLogger();
                var serilogLogger = new SerilogLoggerFactory(loggerSerilog).CreateLogger("SerilogTest");
                SFLoggerFactory.SetCustomLogger(serilogLogger);
                _logFile = "test_serilog.log";
            }
        }

        [TestFixture]
        public class NlogTest : SFBaseLoggerTest
        {
            [OneTimeSetUp]
            public void SetUp()
            {
                var factory = LoggerFactory.Create(
                    builder => builder
                    .AddNLog("TestNLog.config")
                    .SetMinimumLevel(LogLevel.Trace));

                var nlogLogger = factory.CreateLogger("NlogTest");
                SFLoggerFactory.SetCustomLogger(nlogLogger);
                _logFile = "test_nlog.log";
            }
        }
    }
}
