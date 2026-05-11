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
using Snowflake.Data.Tests.Util;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class EasyLoggingITFixture : IDisposable
    {
        public readonly string WorkingDirectory = Path.Combine(Path.GetTempPath(), $"easy_logging_test_configs_{Path.GetRandomFileName()}");

        public EasyLoggingITFixture()
        {
            if (!Directory.Exists(WorkingDirectory))
            {
                Directory.CreateDirectory(WorkingDirectory);
            }
        }

        public void Dispose()
        {
            EasyLoggingStarter.Instance.Reset(EasyLoggingLogLevel.Off);
            Directory.Delete(WorkingDirectory, true);
        }
    }

    public class EasyLoggingIT : SFBaseTest, IClassFixture<EasyLoggingITFixture>, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        private readonly EasyLoggingITFixture _classFixture;
        private const string LogDirectoryName = "dotnet";

        public EasyLoggingIT(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture, EasyLoggingITFixture classFixture) : base(fixture, envFixture)
        {
            _fixture = fixture;
            _classFixture = classFixture;
        }

        public void Dispose()
        {
            EasyLoggingStarter.Instance.Reset(EasyLoggingLogLevel.Off);

            var logDirectory = Path.Combine(_classFixture.WorkingDirectory, LogDirectoryName);
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
            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory));
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
            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, "random config content");
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

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestReCreateEasyLoggingUnixLogFileWithCustomisedPermissions()
        {
            // arrange
            try
            {
                var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory, "640"));
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

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestReCreateEasyLoggingWindowsLogFileIgnoringCustomisedPermissions()
        {
            // arrange
            try
            {
                var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory, "640"));
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

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestFailToEnableEasyLoggingWhenConfigHasWrongPermissions()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory));
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

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestFailToEnableEasyLoggingWhenLogDirectoryNotAccessible()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", "/"));
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

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestFailToEnableEasyLoggingWhenPathIsAccessibleForGroup()
        {
            // arrange
            var logDirectory = Path.Combine(_classFixture.WorkingDirectory, LogDirectoryName);
            Directory.CreateDirectory(logDirectory);
            Syscall.chmod(logDirectory, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR | FilePermissions.S_IRGRP);

            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory, "640"));
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
