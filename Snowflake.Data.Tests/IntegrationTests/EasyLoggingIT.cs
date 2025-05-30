using System.Data;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix.Native;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Log;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture, NonParallelizable]
    public class EasyLoggingIT : SFBaseTest
    {
        private static readonly string s_workingDirectory = Path.Combine(Path.GetTempPath(), "easy_logging_test_configs_", Path.GetRandomFileName());

        [OneTimeSetUp]
        public static void BeforeAll()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }
        }

        [OneTimeTearDown]
        public static void AfterAll()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [TearDown]
        public static void AfterEach()
        {
            EasyLoggingStarter.Instance.Reset(EasyLoggingLogLevel.Warn);
        }

        [Test]
        public void TestEnableEasyLogging()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory));
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                conn.Open();

                // assert
                Assert.IsTrue(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }

        [Test]
        public void TestFailToEnableEasyLoggingForWrongConfiguration()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(s_workingDirectory, "random config content");
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // assert
                Assert.That(thrown.Message, Does.Contain("Connection string is invalid: Unable to initialize session"));
                Assert.IsFalse(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }

        [Test]
        public void TestFailToEnableEasyLoggingWhenConfigHasWrongPermissions()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            // arrange
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory));
            Syscall.chmod(configFilePath, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IWGRP);
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // assert
                Assert.That(thrown.Message, Does.Contain("Connection string is invalid: Unable to initialize session"));
                Assert.IsFalse(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }

        [Test]
        public void TestFailToEnableEasyLoggingWhenLogDirectoryNotAccessible()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            // arrange
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", "/"));
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // assert
                Assert.That(thrown.Message, Does.Contain("Connection string is invalid: Unable to initialize session"));
                Assert.That(thrown.InnerException.Message, Does.Contain("Failed to create logs directory"));
                Assert.IsFalse(EasyLoggerManager.HasEasyLoggingAppender());
            }

        }
    }
}
