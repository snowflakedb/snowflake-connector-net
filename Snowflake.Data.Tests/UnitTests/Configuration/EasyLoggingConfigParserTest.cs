using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Snowflake.Data.Configuration;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    [TestFixture]
    public class EasyLoggingConfigParserTest
    {
        private const string NotExistingFilePath = "../../../Resources/EasyLogging/not_existing_config.json";
        private const string LogLevel = "info";
        private const string LogPath = "./test-logs/log_file.log";

        [OneTimeSetUp]
        public static void BeforeAll()
        {
            if (!Directory.Exists(WorkingDirectory))
            {
                Directory.CreateDirectory(WorkingDirectory);
            }
        }

        [OneTimeTearDown]
        public static void AfterAll()
        {
            Directory.Delete(WorkingDirectory, true);
        }
        
        [Test]
        public void TestThatParsesConfigFile()
        {
            // arrange
            var parser = new EasyLoggingConfigParser();
            var configFilePath = CreateConfigTempFile(Config(LogLevel, LogPath));

            // act
            var config = parser.Parse(configFilePath);

            // assert
            Assert.IsNotNull(config);
            Assert.IsNotNull(config.CommonProps);
            Assert.AreEqual(LogLevel, config.CommonProps.LogLevel);
            Assert.AreEqual(LogPath, config.CommonProps.LogPath);
        }

        [Test]
        public void TestThatReturnsNullIfFileDoesNotExist(
            [Values(null, "", NotExistingFilePath)]
            string notExistingFilePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var config = parser.Parse(notExistingFilePath);

            // assert
            Assert.IsNull(config);
        }

        [Test, TestCaseSource(nameof(WrongConfigFiles))]
        public void TestThatReturnsNullIfMissingOrInvalidRequiredFields(string filePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();

            // act
            var config = parser.Parse(filePath);

            // assert
            Assert.IsNull(config);
        }

        public static IEnumerable<string> WrongConfigFiles()
        {
            BeforeAll();
            return new[]
            {
                CreateConfigTempFile(EmptyConfig),
                CreateConfigTempFile(EmptyCommonConfig),
                CreateConfigTempFile(Config(null, LogPath)),
                CreateConfigTempFile(Config(LogLevel, null)),
                CreateConfigTempFile(Config("unknown", LogPath)),
            };
        }
    }
}
