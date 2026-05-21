using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [TestFixture, NonParallelizable, Order(2)]
    public class EasyLoggerManagerTest
    {

        private const string InfoMessage = "Easy logging Info message";
        private const string DebugMessage = "Easy logging Debug message";
        private const string WarnMessage = "Easy logging Warn message";
        private const string ErrorMessage = "Easy logging Error message";
        private static readonly string s_logsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        private static AsyncLocal<string> _directoryLogPath = new AsyncLocal<string>();

        [OneTimeTearDown]
        public static void CleanUp()
        {
            EasyLoggerManager.Instance.ResetEasyLogging(EasyLoggingLogLevel.Off);
            RemoveEasyLoggingLogFiles();
        }

        [SetUp]
        public void BeforeEach()
        {
            _directoryLogPath.Value = RandomLogsDirectoryPath();
        }

        [TearDown]
        public void AfterEach()
        {
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, _directoryLogPath.Value);
        }

        [Test]
        public void TestThatChangesLogLevel()
        {
            // arrange
            var logger = SFLoggerFactory.GetSFLogger<SFBlockingChunkDownloaderV3>();

            // act
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Off, _directoryLogPath.Value);

            // assert
            Assert.False(logger.IsDebugEnabled());
            Assert.False(logger.IsInfoEnabled());
            Assert.False(logger.IsWarnEnabled());
            Assert.False(logger.IsErrorEnabled());

            // act
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Error, _directoryLogPath.Value);

            // assert
            Assert.False(logger.IsDebugEnabled());
            Assert.False(logger.IsInfoEnabled());
            Assert.False(logger.IsWarnEnabled());
            Assert.True(logger.IsErrorEnabled());

            // act
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, _directoryLogPath.Value);

            // assert
            Assert.False(logger.IsDebugEnabled());
            Assert.False(logger.IsInfoEnabled());
            Assert.True(logger.IsWarnEnabled());
            Assert.True(logger.IsErrorEnabled());

            // act
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Debug, _directoryLogPath.Value);

            // assert
            Assert.True(logger.IsDebugEnabled());
            Assert.True(logger.IsInfoEnabled());
            Assert.True(logger.IsWarnEnabled());
            Assert.True(logger.IsErrorEnabled());
        }

        [Test]
        public static void TestThatLogsToProperFileWithProperLogLevelOnly()
        {
            // arrange
            var logger = SFLoggerFactory.GetSFLogger<SFBlockingChunkDownloaderV3>();
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Info, _directoryLogPath.Value);

            // act
            logger.Debug(DebugMessage);
            logger.Info(InfoMessage);
            logger.Warn(WarnMessage);
            logger.Error(ErrorMessage);

            // assert
            var logLines = File.ReadLines(FindLogFilePath(_directoryLogPath.Value));
            Assert.That(logLines, Has.Exactly(0).Matches<string>(s => s.Contains(DebugMessage)));
            Assert.That(logLines, Has.Exactly(1).Matches<string>(s => s.Contains(InfoMessage)));
            Assert.That(logLines, Has.Exactly(1).Matches<string>(s => s.Contains(WarnMessage)));
            Assert.That(logLines, Has.Exactly(1).Matches<string>(s => s.Contains(ErrorMessage)));

            // arrange
            File.Delete(FindLogFilePath(_directoryLogPath.Value));
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Debug, _directoryLogPath.Value);

            // act
            logger.Debug(DebugMessage);

            // assert
            logLines = File.ReadLines(FindLogFilePath(_directoryLogPath.Value));
            Assert.That(logLines, Has.Exactly(1).Matches<string>(s => s.Contains(DebugMessage)));
        }

        [Test]
        public async Task TestThatRollsLogIfSizeIsTooBig()
        {
            // arrange
            const int ExpectedBackupLogCount = 2;
            var logger = SFLoggerFactory.GetSFLogger<SFBlockingChunkDownloaderV3>();
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Trace, _directoryLogPath.Value);

            var appenders = SFLoggerImpl.s_appenders;
            appenders.Remove(appenders[0]);
            var randomFileName = $"snowflake_dotnet_{Path.GetRandomFileName()}";
            var logFileName = randomFileName.Substring(0, randomFileName.Length - 4) + ".log";
            var appender = new SFRollingFileAppender(FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance)
            {
                LogFilePath = Path.Combine(_directoryLogPath.Value, logFileName),
                MaximumFileSizeInBytes = 1,
                MaxSizeRollBackups = ExpectedBackupLogCount,
                PatternLayout = EasyLoggerManager.PatternLayout()
            };
            appender.ActivateOptions();
            appenders.Add(appender);

            // act
            for (int i = 0; i < 5; i++)
            {
                logger.Debug(DebugMessage);
                await Task.Delay(1000).ConfigureAwait(false);
            }
            var backupLogs = Directory.GetFiles(_directoryLogPath.Value, $"{logFileName}.*.bak");

            // assert
            Assert.AreEqual(ExpectedBackupLogCount, backupLogs.Length);
        }

        [Test]
        public void TestThatOnlyUnknownFieldsAreLogged()
        {
            // arrange
            string expectedFakeLogField = "fake_log_field";
            string ConfigWithUnknownFields = $@"{{
                    ""common"": {{
                        ""LOG_LEVEL"": ""warn"",
                        ""lOg_PaTh"": ""path"",
                        ""{expectedFakeLogField}_1"": ""abc"",
                        ""{expectedFakeLogField}_2"": ""123""
                    }}
                }}";
            var configFilePath = Guid.NewGuid().ToString() + ".json";
            using (var writer = File.CreateText(configFilePath))
            {
                writer.Write(ConfigWithUnknownFields);
            }
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, _directoryLogPath.Value);
            var parser = new EasyLoggingConfigParser();

            // act
            parser.Parse(configFilePath);

            // assert
            var logLines = File.ReadLines(FindLogFilePath(_directoryLogPath.Value));
            Assert.That(logLines, Has.Exactly(2).Matches<string>(s => s.Contains($"Unknown field from config: {expectedFakeLogField}")));

            // cleanup
            File.Delete(configFilePath);
        }

        private static string RandomLogsDirectoryPath()
        {
            var randomName = Path.GetRandomFileName();
            return Path.Combine(s_logsDirectory, $"easy_logging_logs_{randomName}", "dotnet");
        }

        internal static string FindLogFilePath(string directoryLogPath)
        {
            Assert.True(Directory.Exists(directoryLogPath));
            var files = Directory.GetFiles(directoryLogPath);
            Assert.AreEqual(1, files.Length);
            return files.First();
        }

        private static void RemoveEasyLoggingLogFiles()
        {
            Directory.GetFiles(s_logsDirectory)
                .Where(filePath => filePath.StartsWith(Path.Combine(s_logsDirectory, "easy_logging_logs")))
                .AsParallel()
                .ForAll(filePath => File.Delete(filePath));
        }
    }
}
