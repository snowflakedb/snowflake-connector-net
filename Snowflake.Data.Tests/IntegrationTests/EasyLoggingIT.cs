using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Mono.Unix;
using Mono.Unix.Native;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture, NonParallelizable]
    public class EasyLoggingIT : SFBaseTest
    {
        private static readonly string s_workingDirectory = Path.Combine(Path.GetTempPath(), $"easy_logging_test_configs_{Path.GetRandomFileName()}");
        private const string LogDirectoryName = "dotnet";

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
            EasyLoggingStarter.Instance.Reset(EasyLoggingLogLevel.Off);
            Directory.Delete(s_workingDirectory, true);
        }

        [TearDown]
        public static void AfterEach()
        {
            EasyLoggingStarter.Instance.Reset(EasyLoggingLogLevel.Off);
 
            var logDirectory = Path.Combine(s_workingDirectory, LogDirectoryName);
            if (Directory.Exists(logDirectory))
            {
                if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    Syscall.chmod(logDirectory, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR);
                }
            }
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
        [Platform(Exclude = "Win")]
        public void TestReCreateEasyLoggingUnixLogFileWithCustomisedPermissions()
        {
            // arrange
            try
            {
                var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory, "640"));
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";
                    conn.Open();
                    var sfLogger = (SFLoggerImpl)SFLoggerFactory.GetSFLogger<EasyLoggingIT>();
                    var fileAppender = (SFRollingFileAppender)SFLoggerImpl.s_appenders.First();
                    var logFile = fileAppender.LogFilePath;
                    File.Delete(logFile);

                    // act
                    sfLogger.Warn("This is a warning message");
                    sfLogger.Warn("This is another warning message");

                    // assert
                    var logFilePermissions = UnixOperations.Instance.GetFilePermissions(logFile);
                    Assert.AreEqual(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.GroupRead,
                        logFilePermissions);
                    var logs = FileOperations.Instance.ReadAllText(logFile);
                    Assert.That(logs, Does.Contain("This is a warning message"));
                    Assert.That(logs, Does.Contain("This is another warning message"));
                }
            }
            finally
            {
                EasyLoggingStarter.Instance._logFileUnixPermissions = EasyLoggingStarter.DefaultFileUnixPermissions;
            }
        }

        [Test]
        [Platform("Win")]
        public void TestReCreateEasyLoggingWindowsLogFileIgnoringCustomisedPermissions()
        {
            // arrange
            try
            {
                var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory, "640"));
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";
                    conn.Open();
                    var sfLogger = (SFLoggerImpl)SFLoggerFactory.GetSFLogger<EasyLoggingIT>();
                    var fileAppender = (SFRollingFileAppender)SFLoggerImpl.s_appenders.First();
                    var logFile = fileAppender.LogFilePath;
                    File.Delete(logFile);

                    // act
                    sfLogger.Warn("This is a warning message");
                    sfLogger.Warn("This is another warning message");

                    // assert
                    var logs = FileOperations.Instance.ReadAllText(logFile);
                    Assert.That(logs, Does.Contain("This is a warning message"));
                    Assert.That(logs, Does.Contain("This is another warning message"));
                }
            }
            finally
            {
                EasyLoggingStarter.Instance._logFileUnixPermissions = EasyLoggingStarter.DefaultFileUnixPermissions;
            }
        }

        [Test]
        [Platform(Exclude = "Win")]
        public void TestFailToEnableEasyLoggingWhenConfigHasWrongPermissions()
        {
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
        [Platform(Exclude = "Win")]
        public void TestFailToEnableEasyLoggingWhenLogDirectoryNotAccessible()
        {
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

        [Test]
        [Platform(Exclude = "Win")]
        public void TestFailToEnableEasyLoggingWhenPathIsAccessibleForOthers()
        {
            // arrange
            var logDirectory = Path.Combine(s_workingDirectory, LogDirectoryName);
            Directory.CreateDirectory(logDirectory);
            Syscall.chmod(logDirectory, FilePermissions.S_IRGRP | FilePermissions.S_IWGRP | FilePermissions.S_IXGRP);

            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory, "640"));
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                Console.WriteLine(thrown.Message);
                // assert
                Assert.That(thrown.Message, Does.Contain("Connection string is invalid: Unable to initialize session"));
                Assert.That(thrown.InnerException.Message, Does.Contain("Too broad access permissions for logs directory"));
                Assert.IsFalse(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }
    }
}
