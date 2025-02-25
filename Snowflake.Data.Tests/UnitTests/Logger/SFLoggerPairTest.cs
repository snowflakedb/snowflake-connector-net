/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;
using Snowflake.Data.Client;
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
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, "STDOUT");
        }

        [SetUp]
        public void BeforeTest()
        {
            _loggerPair = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
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
            _loggerPair = SFLoggerFactory.GetLogger<SFLoggerPairTest>();
            Assert.IsInstanceOf<SFLoggerPair>(_loggerPair);
        }

        [Test]
        public void TestIsDebugEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _loggerPair = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _loggerPair.IsDebugEnabled());
            _loggerPair.Debug("debug log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsInfoEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _loggerPair = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _loggerPair.IsInfoEnabled());
            _loggerPair.Info("info log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsWarnEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _loggerPair = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _loggerPair.IsWarnEnabled());
            _loggerPair.Warn("warn log message", new Exception("test exception"));
        }

        [Test]
        public void TestIsErrorEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _loggerPair = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _loggerPair.IsErrorEnabled());
            _loggerPair.Error("error log message", new Exception("test exception"));
        }

        [Test]
        public void TestGetAppenders()
        {
            Assert.Throws<NotImplementedException>(() => _loggerPair.GetAppenders());
        }

        [Test]
        public void TestAddAppender()
        {
            Assert.Throws<NotImplementedException>(() => _loggerPair.AddAppender(new SFConsoleAppender()));
        }

        [Test]
        public void TestRemoveAppender()
        {
            Assert.Throws<NotImplementedException>(() => _loggerPair.RemoveAppender(new SFConsoleAppender()));
        }

        [Test]
        public void TestSetLevel()
        {
            Assert.Throws<NotImplementedException>(() => _loggerPair.SetLevel(LoggingEvent.DEBUG));
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

            return SFLoggerFactory.GetLogger<SFLoggerPairTest>();
        }
    }
}
