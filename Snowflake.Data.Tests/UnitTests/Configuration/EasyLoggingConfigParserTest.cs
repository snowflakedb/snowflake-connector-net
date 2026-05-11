using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Mono.Unix;
using Moq;
using Xunit;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    public class EasyLoggingConfigParserTest
    {
        private const string NotExistingFilePath = "../../../Resources/EasyLogging/not_existing_config.json";
        private const string LogLevel = "info";
        private const string LogPath = "./test-logs/log_file.log";
        private static readonly string s_workingDirectory = Path.Combine(Path.GetTempPath(), "easy_logging_test_configs_", Path.GetRandomFileName());
        public static void BeforeAll()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }
        }
        public static void AfterAll()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("640")]
        public void TestThatParsesConfigFile(string logFileUnixPermissions)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config(LogLevel, LogPath, logFileUnixPermissions));

            // act
            var config = parser.Parse(configFilePath);

            // assert
            Assert.NotNull(config);
            Assert.NotNull(config.CommonProps);
            Assert.Equal(LogLevel, config.CommonProps.LogLevel);
            Assert.Equal(LogPath, config.CommonProps.LogPath);
            if (logFileUnixPermissions == null)
                Assert.Null(config.Dotnet);
            else
            {
                Assert.NotNull(config.Dotnet);
                Assert.Equal(logFileUnixPermissions, config.Dotnet.LogFileUnixPermissions);
            }
        }

        [Fact]
        public void TestThatThrowsExceptionForInvalidPermissionValue()
        {
            // arrange
            var invalidValue = "800";
            var parser = new EasyLoggingConfigParser();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config(LogLevel, LogPath, invalidValue));

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(configFilePath));

            // assert
            Assert.NotNull(thrown);
            Assert.Contains($"Parsing easy logging configuration failed", thrown.Message);
        }

        [Fact]
        public void TestThatThrowsExceptionForIncorrectPermissionValueType()
        {
            // arrange
            var incorrectValueType = "abc";
            var parser = new EasyLoggingConfigParser();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config(LogLevel, LogPath, incorrectValueType));

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(configFilePath));

            // assert
            Assert.NotNull(thrown);
            Assert.Contains($"Parsing easy logging configuration failed", thrown.Message);
        }

        [Theory, MemberData(nameof(ConfigFilesWithoutValues))]
        public void TestThatParsesConfigFileWithNullValues(string filePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var config = parser.Parse(filePath);

            // assert
            Assert.NotNull(config);
            Assert.NotNull(config.CommonProps);
            Assert.Null(config.CommonProps.LogLevel);
            Assert.Null(config.CommonProps.LogPath);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void TestThatReturnsNullWhenNothingToParse(string noFilePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var config = parser.Parse(noFilePath);

            // assert
            Assert.Null(config);
        }

        [Fact]
        public void TestThatFailsWhenTheFileDoesNotExist()
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(NotExistingFilePath));

            // assert
            Assert.NotNull(thrown);
            Assert.True(thrown.Message.Contains("Finding easy logging configuration failed"));
        }

        [Theory, MemberData(nameof(WrongConfigFiles))]
        public void TestThatFailsIfMissingOrInvalidRequiredFields(string filePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(filePath));
            // assert
            Assert.NotNull(thrown);
            Assert.NotEmpty(thrown.Message);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestThatConfigFileIsNotUsedIfOthersCanModifyTheConfigFile()
        {
            // arrange
            var unixOperations = new Mock<UnixOperations>();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, null);
            var stream = new UnixFileInfo(configFilePath).OpenRead();
            var parser = new EasyLoggingConfigParser(unixOperations.Object);
            unixOperations
                .Setup(u => u.CheckFileHasAnyOfPermissions(stream.FileAccessPermissions,
                    It.Is<FileAccessPermissions>(p => p.Equals(FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite))))
                .Returns(true);
            unixOperations
                .Setup(u => u.GetCurrentUserId())
                .Returns(stream.OwnerUserId);
            unixOperations
                .Setup(u => u.GetCurrentGroupId())
                .Returns(stream.OwnerGroupId);

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(configFilePath));

            // assert
            Assert.NotNull(thrown);
            Assert.NotEmpty(thrown.Message);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestThatConfigFileIsNotUsedIfUserDoesNotOwnConfigFile()
        {
            // arrange
            var unixOperations = new Mock<UnixOperations>();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, null);
            var stream = new UnixFileInfo(configFilePath).OpenRead();
            var parser = new EasyLoggingConfigParser(unixOperations.Object);
            unixOperations
                .Setup(u => u.GetCurrentUserId())
                .Returns(stream.OwnerUserId - 1);

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(configFilePath));

            // assert
            Assert.NotNull(thrown);
            Assert.NotEmpty(thrown.Message);
        }

        [FactSkipOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestThatConfigFileIsNotUsedIfGroupDoesNotOwnConfigFile()
        {
            // arrange
            var unixOperations = new Mock<UnixOperations>();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, null);
            var stream = new UnixFileInfo(configFilePath).OpenRead();
            var parser = new EasyLoggingConfigParser(unixOperations.Object);
            unixOperations
                .Setup(u => u.GetCurrentUserId())
                .Returns(stream.OwnerUserId);
            unixOperations
                .Setup(u => u.GetCurrentGroupId())
                .Returns(stream.OwnerGroupId - 1);

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(configFilePath));

            // assert
            Assert.NotNull(thrown);
            Assert.NotEmpty(thrown.Message);
        }

        public static IEnumerable<object[]> ConfigFilesWithoutValues()
        {
            BeforeAll();
            return new[]
            {
                CreateConfigTempFile(s_workingDirectory, EmptyCommonConfig),
                CreateConfigTempFile(s_workingDirectory, Config(null, null))
            }.Select(f => new object[] { f });
        }

        public static IEnumerable<object[]> WrongConfigFiles()
        {
            BeforeAll();
            return new[]
            {
                CreateConfigTempFile(s_workingDirectory, EmptyConfig),
                CreateConfigTempFile(s_workingDirectory, Config("unknown", LogPath)),
            }.Select(f => new object[] { f });
        }
    }
}
