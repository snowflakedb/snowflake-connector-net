using Snowflake.Data.Configuration;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Log;

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
            SFLoggerFactory.useDefaultLogger();
            SFLoggerFactory.enableLogger();
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
