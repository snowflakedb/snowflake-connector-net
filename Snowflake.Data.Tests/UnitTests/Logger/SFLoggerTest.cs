/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;
using System;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class SFLoggerTest
    {
        SFLogger _logger;

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
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, "STDOUT");
        }

        [TearDown]
        public void AfterTest()
        {
            // Return to default setting
            SFLoggerFactory.UseDefaultSFLogger();
        }

        [Test]
        public void TestUsingSFLogger()
        {
            SFLoggerFactory.UseDefaultSFLogger();
            _logger = SFLoggerFactory.GetSFLogger<SFLoggerTest>();
            Assert.IsInstanceOf<SFLoggerImpl>(_logger);
        }

        [Test]
        public void TestUsingEmptyLogger()
        {
            SFLoggerFactory.UseEmptySFLogger();
            _logger = SFLoggerFactory.GetSFLogger<SFLoggerTest>();
            Assert.IsInstanceOf<SFLoggerEmptyImpl>(_logger);
        }

        [Test]
        public void TestIsDebugEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsDebugEnabled());
            _logger.Debug("debug log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsInfoEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsInfoEnabled());
            _logger.Info("info log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsWarnEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsWarnEnabled());
            _logger.Warn("warn log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsErrorEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsErrorEnabled());
            _logger.Error("error log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsFatalEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsFatalEnabled());
            _logger.Fatal("fatal log message", new Exception("test exception"));
        }

        [Test]
        public void TestGetAppenders(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            if (isEnabled)
            {
                var appenders = _logger.GetAppenders();
                Assert.IsInstanceOf<SFConsoleAppender>(appenders[0]);
            }
            else
            {
                Assert.Throws<NotImplementedException>(() => _logger.GetAppenders());
            }
        }

        [Test]
        public void TestAddAppender(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            if (isEnabled)
            {
                var appenders = _logger.GetAppenders();
                Assert.AreEqual(1, appenders.Count);
                _logger.AddAppender(new SFConsoleAppender());
                Assert.AreEqual(2, appenders.Count);
            }
            else
            {
                Assert.Throws<NotImplementedException>(() => _logger.AddAppender(new SFConsoleAppender()));
            }
        }

        [Test]
        public void TestRemoveAppender(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            if (isEnabled)
            {
                var appenders = _logger.GetAppenders();
                Assert.AreEqual(1, appenders.Count);
                _logger.RemoveAppender(appenders[0]);
                Assert.AreEqual(0, appenders.Count);
            }
            else
            {
                Assert.Throws<NotImplementedException>(() => _logger.RemoveAppender(new SFConsoleAppender()));
            }
        }

        [Test]
        public void TestSetLevel(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);
            if (isEnabled)
            {
                _logger.SetLevel(LoggingEvent.DEBUG);
                Assert.AreEqual(LoggingEvent.DEBUG, ((SFLoggerImpl)_logger)._level);
            }
            else
            {
                Assert.Throws<NotImplementedException>(() => _logger.SetLevel(LoggingEvent.DEBUG));
            }
        }

        private SFLogger GetLogger(bool isEnabled)
        {
            if (isEnabled)
            {
                SFLoggerFactory.UseDefaultSFLogger();
            }
            else
            {
                SFLoggerFactory.UseEmptySFLogger();
            }

            return SFLoggerFactory.GetSFLogger<SFLoggerTest>(false);
        }
    }
}
