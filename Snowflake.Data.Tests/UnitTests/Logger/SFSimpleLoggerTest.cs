/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class SFSimpleLoggerTest
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
            SFLoggerFactory.EnableSimpleLogger();
        }

        [Test]
        public void TestUsingSimpleLogger()
        {
            SFLoggerFactory.EnableSimpleLogger();
            _logger = SFLoggerFactory.GetSimpleLogger<SFLoggerTest>();
            Assert.IsInstanceOf<Log4NetImpl>(_logger);
        }

        [Test]
        public void TestSettingCustomLogger()
        {
            SFLoggerFactory.DisableSimpleLogger();
            _logger = SFLoggerFactory.GetSimpleLogger<SFLoggerTest>();
            Assert.IsInstanceOf<SFLoggerEmptySimpleImpl>(_logger);
        }

        [Test]
        public void TestIsDebugEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsDebugEnabled());
        }

        [Test]
        public void TestIsInfoEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsInfoEnabled());
        }

        [Test]
        public void TestIsWarnEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsWarnEnabled());
        }

        [Test]
        public void TestIsErrorEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsErrorEnabled());
        }

        [Test]
        public void TestIsFatalEnabled(
            [Values(false, true)] bool isEnabled)
        {
            _logger = GetLogger(isEnabled);

            Assert.AreEqual(isEnabled, _logger.IsFatalEnabled());
        }

        private SFLogger GetLogger(bool isEnabled)
        {
            if (isEnabled)
            {
                SFLoggerFactory.EnableSimpleLogger();
            }
            else
            {
                SFLoggerFactory.DisableSimpleLogger();
            }

            return SFLoggerFactory.GetSimpleLogger<SFLoggerTest>();
        }
    }
}
