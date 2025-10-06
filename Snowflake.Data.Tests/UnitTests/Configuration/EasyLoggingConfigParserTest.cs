using System;
using System.Collections.Generic;
using System.IO;
using Mono.Unix;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    [TestFixture, NonParallelizable]
    public class EasyLoggingConfigParserTest
    {
        private const string NotExistingFilePath = "../../../Resources/EasyLogging/not_existing_config.json";
        private const string LogLevel = "info";
        private const string LogPath = "./test-logs/log_file.log";
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

        [Test]
        [TestCase(null)]
        [TestCase("640")]
        public void TestThatParsesConfigFile(string logFileUnixPermissions)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config(LogLevel, LogPath, logFileUnixPermissions));

            // act
            var config = parser.Parse(configFilePath);

            // assert
            Assert.IsNotNull(config);
            Assert.IsNotNull(config.CommonProps);
            Assert.AreEqual(LogLevel, config.CommonProps.LogLevel);
            Assert.AreEqual(LogPath, config.CommonProps.LogPath);
            if (logFileUnixPermissions == null)
                Assert.IsNull(config.Dotnet);
            else
            {
                Assert.IsNotNull(config.Dotnet);
                Assert.AreEqual(logFileUnixPermissions, config.Dotnet.LogFileUnixPermissions);
            }
        }

        [Test]
        public void TestThatThrowsExceptionForInvalidPermissionValue()
        {
            // arrange
            var invalidValue = "800";
            var parser = new EasyLoggingConfigParser();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config(LogLevel, LogPath, invalidValue));

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(configFilePath));

            // assert
            Assert.IsNotNull(thrown);
            Assert.That(thrown.Message, Does.Contain($"Parsing easy logging configuration failed"));
        }

        [Test]
        public void TestThatThrowsExceptionForIncorrectPermissionValueType()
        {
            // arrange
            var incorrectValueType = "abc";
            var parser = new EasyLoggingConfigParser();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config(LogLevel, LogPath, incorrectValueType));

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(configFilePath));

            // assert
            Assert.IsNotNull(thrown);
            Assert.That(thrown.Message, Does.Contain($"Parsing easy logging configuration failed"));
        }

        [Test, TestCaseSource(nameof(ConfigFilesWithoutValues))]
        public void TestThatParsesConfigFileWithNullValues(string filePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var config = parser.Parse(filePath);

            // assert
            Assert.IsNotNull(config);
            Assert.IsNotNull(config.CommonProps);
            Assert.IsNull(config.CommonProps.LogLevel);
            Assert.IsNull(config.CommonProps.LogPath);
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        public void TestThatReturnsNullWhenNothingToParse(string noFilePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var config = parser.Parse(noFilePath);

            // assert
            Assert.IsNull(config);
        }

        [Test]
        public void TestThatFailsWhenTheFileDoesNotExist()
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(NotExistingFilePath));

            // assert
            Assert.IsNotNull(thrown);
            Assert.IsTrue(thrown.Message.Contains("Finding easy logging configuration failed"));
        }

        [Test, TestCaseSource(nameof(WrongConfigFiles))]
        public void TestThatFailsIfMissingOrInvalidRequiredFields(string filePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var thrown = Assert.Throws<Exception>(() => parser.Parse(filePath));
            // assert
            Assert.IsNotNull(thrown);
            Assert.AreEqual(thrown.Message, "Parsing easy logging configuration failed");
        }

        [Test]
        [Platform(Exclude = "Win")]
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
            Assert.IsNotNull(thrown);
            Assert.AreEqual(thrown.Message, "Finding easy logging configuration failed: Error due to other users having permission to modify the config file");
        }

        [Test]
        [Platform(Exclude = "Win")]
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
            Assert.IsNotNull(thrown);
            Assert.AreEqual(thrown.Message, "Finding easy logging configuration failed: Error due to user not having ownership of the config file");
        }

        [Test]
        [Platform(Exclude = "Win")]
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
            Assert.IsNotNull(thrown);
            Assert.AreEqual(thrown.Message, "Finding easy logging configuration failed: Error due to group not having ownership of the config file");
        }

        public static IEnumerable<string> ConfigFilesWithoutValues()
        {
            BeforeAll();
            return new[]
            {
                CreateConfigTempFile(s_workingDirectory, EmptyCommonConfig),
                CreateConfigTempFile(s_workingDirectory, Config(null, null))
            };
        }

        public static IEnumerable<string> WrongConfigFiles()
        {
            BeforeAll();
            return new[]
            {
                CreateConfigTempFile(s_workingDirectory, EmptyConfig),
                CreateConfigTempFile(s_workingDirectory, Config("unknown", LogPath)),
            };
        }
    }
}
