using NUnit.Framework;
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
            Assert.IsInstanceOf<SFLoggerImpl>(_logger);
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

            Assert.AreEqual(isEnabled, _logger.IsDebugEnabled());
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

            Assert.AreEqual(isEnabled, _logger.IsInfoEnabled());
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

            Assert.AreEqual(isEnabled, _logger.IsWarnEnabled());
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

            Assert.AreEqual(isEnabled, _logger.IsErrorEnabled());
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
                Assert.AreEqual(logLevel, SFLoggerImpl.s_level);

                if (logLevel == LoggingEvent.OFF)
                {
                    Assert.IsFalse(_logger.IsDebugEnabled());
                    Assert.IsFalse(_logger.IsInfoEnabled());
                    Assert.IsFalse(_logger.IsWarnEnabled());
                    Assert.IsFalse(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.TRACE)
                {
                    Assert.IsTrue(_logger.IsDebugEnabled());
                    Assert.IsTrue(_logger.IsInfoEnabled());
                    Assert.IsTrue(_logger.IsWarnEnabled());
                    Assert.IsTrue(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.DEBUG)
                {
                    Assert.IsTrue(_logger.IsDebugEnabled());
                    Assert.IsTrue(_logger.IsInfoEnabled());
                    Assert.IsTrue(_logger.IsWarnEnabled());
                    Assert.IsTrue(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.INFO)
                {
                    Assert.IsFalse(_logger.IsDebugEnabled());
                    Assert.IsTrue(_logger.IsInfoEnabled());
                    Assert.IsTrue(_logger.IsWarnEnabled());
                    Assert.IsTrue(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.WARN)
                {
                    Assert.IsFalse(_logger.IsDebugEnabled());
                    Assert.IsFalse(_logger.IsInfoEnabled());
                    Assert.IsTrue(_logger.IsWarnEnabled());
                    Assert.IsTrue(_logger.IsErrorEnabled());
                }
                else if (logLevel == LoggingEvent.ERROR)
                {
                    Assert.IsFalse(_logger.IsDebugEnabled());
                    Assert.IsFalse(_logger.IsInfoEnabled());
                    Assert.IsFalse(_logger.IsWarnEnabled());
                    Assert.IsTrue(_logger.IsErrorEnabled());
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
