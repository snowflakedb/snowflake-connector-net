﻿/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using log4net.Repository.Hierarchy;
    using log4net;
    using NUnit.Framework;
    using Snowflake.Data.Log;
    using log4net.Core;
    using System;

    [TestFixture]
    class SFLoggerTest
    {
        SFLogger _logger;

        [OneTimeSetUp]
        public void BeforeTest()
        {
            // Log level defaults to Warn on net6.0 builds in github actions
            // Set the root level to Debug
            ((Hierarchy)LogManager.GetRepository()).Root.Level = Level.Debug;
            ((Hierarchy)LogManager.GetRepository()).RaiseConfigurationChanged(EventArgs.Empty);
        }

        [TearDown] public void AfterTest()
        {
            // Return to default setting
            SFLoggerFactory.useDefaultLogger();
            SFLoggerFactory.enableLogger();
        }

        [Test]
        [Ignore("SFLoggerTest")]
        public void SFLoggerTestDone()
        {
            // Do nothing;
        }

        [Test]
        public void TestUsingDefaultLogger()
        {
            SFLoggerFactory.useDefaultLogger();
            _logger = SFLoggerFactory.GetLogger<SFLoggerTest>();
            Assert.IsInstanceOf<Log4NetImpl>(_logger);
        }

        [Test]
        public void TestSettingCustomLogger()
        {
            SFLoggerFactory.Instance(new SFLoggerEmptyImpl());
            _logger = SFLoggerFactory.GetLogger<SFLoggerTest>();
            Assert.IsInstanceOf<SFLoggerEmptyImpl>(_logger);
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
                SFLoggerFactory.enableLogger();
            }
            else
            {
                SFLoggerFactory.disableLogger();
            }

            return SFLoggerFactory.GetLogger<SFLoggerTest>();
        }
    }
}
