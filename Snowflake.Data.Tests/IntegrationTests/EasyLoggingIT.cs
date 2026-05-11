using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Mono.Unix;
using Mono.Unix.Native;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public class EasyLoggingIT : SFBaseTest
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public EasyLoggingIT(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture) : base(fixture, envFixture) { _fixture = fixture; }

        private static readonly string s_workingDirectory = Path.Combine(Path.GetTempPath(), $"easy_logging_test_configs_{Path.GetRandomFileName()}");
        private const string LogDirectoryName = "dotnet";
        public static void BeforeAll()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }
        }
        public static void AfterAll()
        {
            EasyLoggingStarter.Instance.Reset(EasyLoggingLogLevel.Off);
            Directory.Delete(s_workingDirectory, true);
        }
        public static void AfterEach()
        {
            EasyLoggingStarter.Instance.Reset(EasyLoggingLogLevel.Off);

            var logDirectory = Path.Combine(s_workingDirectory, LogDirectoryName);
            if (Directory.Exists(logDirectory))
            {
                if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var result = Syscall.chmod(logDirectory, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR);
                    if (result != 0)
                    {
                        throw new Exception($"Failed to restore directory permissions in teardown for {logDirectory}");
                    }
                }
            }
        }

        [Fact]
        public void TestEnableEasyLogging()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory));
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                conn.Open();

                // assert
                Assert.True(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }

        [Fact]
        public void TestFailToEnableEasyLoggingForWrongConfiguration()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(s_workingDirectory, "random config content");
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // assert
                Assert.Contains("Connection string is invalid: Unable to initialize session", thrown.Message);
                Assert.False(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }

        [Fact]
        public void TestReCreateEasyLoggingUnixLogFileWithCustomisedPermissions()
        {
            // arrange
            try
            {
                var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory, "640"));
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";
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
                    Assert.Equal(FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite | FileAccessPermissions.GroupRead,
                        logFilePermissions);
                    var logs = FileOperations.Instance.ReadAllText(logFile);
                    Assert.Contains("This is a warning message", logs);
                    Assert.Contains("This is another warning message", logs);
                }
            }
            finally
            {
                EasyLoggingStarter.Instance._logFileUnixPermissions = EasyLoggingStarter.DefaultFileUnixPermissions;
            }
        }

        [Fact]
        public void TestReCreateEasyLoggingWindowsLogFileIgnoringCustomisedPermissions()
        {
            // arrange
            try
            {
                var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory, "640"));
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";
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
                    Assert.Contains("This is a warning message", logs);
                    Assert.Contains("This is another warning message", logs);
                }
            }
            finally
            {
                EasyLoggingStarter.Instance._logFileUnixPermissions = EasyLoggingStarter.DefaultFileUnixPermissions;
            }
        }

        [Fact]
        public void TestFailToEnableEasyLoggingWhenConfigHasWrongPermissions()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory));
            Syscall.chmod(configFilePath, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IWGRP);
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // assert
                Assert.Contains("Connection string is invalid: Unable to initialize session", thrown.Message);
                Assert.False(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }

        [Fact]
        public void TestFailToEnableEasyLoggingWhenLogDirectoryNotAccessible()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", "/"));
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // assert
                Assert.Contains("Connection string is invalid: Unable to initialize session", thrown.Message);
                Assert.Contains("Failed to create logs directory", thrown.InnerException.Message);
                Assert.False(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }

        [Fact]
        public void TestFailToEnableEasyLoggingWhenPathIsAccessibleForGroup()
        {
            // arrange
            var logDirectory = Path.Combine(s_workingDirectory, LogDirectoryName);
            Directory.CreateDirectory(logDirectory);
            Syscall.chmod(logDirectory, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR | FilePermissions.S_IRGRP);

            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config("WARN", s_workingDirectory, "640"));
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // assert
                Assert.Contains("Connection string is invalid: Unable to initialize session", thrown.Message);
                Assert.Contains("Too broad access permissions for logs directory", thrown.InnerException.Message);
                Assert.False(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }
    }
}
