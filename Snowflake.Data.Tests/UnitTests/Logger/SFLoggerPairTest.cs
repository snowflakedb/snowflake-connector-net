using NUnit.Framework;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;
using System;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class SFLoggerPairTest
    {
        SFLogger _loggerPair;

        [OneTimeSetUp]
        public static void BeforeAll()
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

        [SetUp]
        public void BeforeTest()
        {
            _loggerPair = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
        }

        [Test]
        public void TestUsingSFLogger()
        {
            _loggerPair = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
            Assert.IsInstanceOf<SFLoggerPair>(_loggerPair);
        }

        [Test]
        public void TestIsDebugEnabled(
            [Values(false, true)] bool isEnabled)
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

            Assert.AreEqual(isEnabled, _loggerPair.IsDebugEnabled());
            _loggerPair.Debug("debug log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsInfoEnabled(
            [Values(false, true)] bool isEnabled)
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

            Assert.AreEqual(isEnabled, _loggerPair.IsInfoEnabled());
            _loggerPair.Info("info log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsWarnEnabled(
            [Values(false, true)] bool isEnabled)
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

            Assert.AreEqual(isEnabled, _loggerPair.IsWarnEnabled());
            _loggerPair.Warn("warn log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsErrorEnabled(
            [Values(false, true)] bool isEnabled)
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

            Assert.AreEqual(isEnabled, _loggerPair.IsErrorEnabled());
            _loggerPair.Error("error log message", new Exception("test exception"));
        }

        private SFLogger GetLogger()
        {
            var logger = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
            EasyLoggerManager.AddConsoleAppender();
            return logger;
        }
    }
}
