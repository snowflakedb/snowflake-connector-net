using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
    [CollectionDefinition(nameof(EasyLoggingITTestFixture), DisableParallelization = true)]
    public sealed class EasyLoggingITTestFixture : ICollectionFixture<EasyLoggingITTestFixture.Fixture>
    {
        public sealed class Fixture : IDisposable
        {
            public readonly string WorkingDirectory = Path.Combine(Path.GetTempPath(), $"easy_logging_test_configs_{Path.GetRandomFileName()}");

            public Fixture()
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
    }

    [Collection(nameof(EasyLoggingITTestFixture))]
    public class EasyLoggingIT : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        private readonly EasyLoggingITTestFixture.Fixture _classFixture;
        private readonly List<SFAppender> _appendersPriorToTest;
        private const string LogDirectoryName = "dotnet";

        public EasyLoggingIT(SFBaseTestAsyncFixture fixture, EasyLoggingITTestFixture.Fixture classFixture) : base(fixture)
        {
            _fixture = fixture;
            _classFixture = classFixture;

            _appendersPriorToTest = SFLoggerImpl.s_appenders.ToList();
            SFLoggerImpl.s_appenders = new List<SFAppender>();
        }

        public void Dispose()
        {
            EasyLoggingStarter.Instance.Reset(EasyLoggingLogLevel.Off);
            SFLoggerImpl.s_appenders = _appendersPriorToTest;

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

        [SFFact]
        public async Task TestEnableEasyLogging()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory));
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                await conn.OpenAsync(CancellationToken.None);

                // assert
                Assert.True(EasyLoggerManager.HasEasyLoggingAppender());
            }
        }

        [SFFact]
        public async Task TestFailToEnableEasyLoggingForWrongConfiguration()
        {
            #if NETFRAMEWORK
            Skip.When(true, "Not on framework");
            #endif

            // arrange
            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, "random config content");
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.OpenAsync(CancellationToken.None));

                // assert
                var messages = new[] {thrown.Message, thrown.InnerException?.Message};
                var concatenatedMessages = string.Join(Environment.NewLine, messages);
                Assert.Contains("Connection string is invalid: Unable to initialize session", concatenatedMessages);
                Assert.Empty(SFLoggerImpl.s_appenders);
            }
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public async Task TestReCreateEasyLoggingUnixLogFileWithCustomisedPermissions()
        {
            // arrange
            try
            {
                var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory, "640"));
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public async Task TestReCreateEasyLoggingWindowsLogFileIgnoringCustomisedPermissions()
        {
            // arrange
            try
            {
                var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory, "640"));
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFFact(SkipCondition.SkipOnWindows)]
        public async Task TestFailToEnableEasyLoggingWhenConfigHasWrongPermissions()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory));
            Syscall.chmod(configFilePath, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IWGRP);
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.OpenAsync(CancellationToken.None));

                // assert
                var messages = thrown.Message + "/n" + thrown.InnerException.Message;
                Assert.Contains("Connection string is invalid: Unable to initialize session", messages);
                Assert.Empty(SFLoggerImpl.s_appenders);
            }
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public async Task TestFailToEnableEasyLoggingWhenLogDirectoryNotAccessible()
        {
            // arrange
            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", "/"));
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                Exception thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.OpenAsync(CancellationToken.None));

                // assert
                var sb = new StringBuilder();
                for (;;)
                {
                    sb.Append(thrown.Message);
                    thrown = thrown.InnerException;
                    if (thrown == null) break;
                }

                var message = sb.ToString();
                Assert.Contains("Connection string is invalid: Unable to initialize session", message);
                Assert.Contains("Failed to create logs directory", message);
                Assert.Empty(SFLoggerImpl.s_appenders);
            }
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public async Task TestFailToEnableEasyLoggingWhenPathIsAccessibleForGroup()
        {
            // arrange
            var logDirectory = Path.Combine(_classFixture.WorkingDirectory, LogDirectoryName);
            Directory.CreateDirectory(logDirectory);
            Syscall.chmod(logDirectory, FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR | FilePermissions.S_IRGRP);

            var configFilePath = CreateConfigTempFile(_classFixture.WorkingDirectory, Config("WARN", _classFixture.WorkingDirectory, "640"));
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + $"CLIENT_CONFIG_FILE={configFilePath}";

                // act
                Exception thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.OpenAsync(CancellationToken.None));

                // assert
                var sb = new StringBuilder();
                for (;;)
                {
                    sb.Append(thrown.Message);
                    thrown = thrown.InnerException;
                    if (thrown == null) break;
                }

                var messages = sb.ToString();
                Assert.Contains("Connection string is invalid: Unable to initialize session", messages);
                Assert.Contains("Too broad access permissions for logs directory", messages);
                Assert.Empty(SFLoggerImpl.s_appenders);
            }
        }
    }
}
