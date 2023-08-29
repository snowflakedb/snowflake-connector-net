using NUnit.Framework;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    [TestFixture]
    public class EasyLoggingConfigParserTest
    {
        private const string ConfigFilePath = "../../../Resources/EasyLogging/config.json";
        private const string NotExistingFilePath = "../../../Resources/EasyLogging/not_existing_config.json";
        private const string EmptyConfigFilePath = "../../../Resources/EasyLogging/empty_config.json";
        private const string ConfigWithoutLogLevelFilePath = "../../../Resources/EasyLogging/config_without_log_level.json";
        private const string ConfigWithNullLogLevelFilePath = "../../../Resources/EasyLogging/config_with_null_log_level.json";
        private const string ConfigWithEmptyLogLevelFilePath = "../../../Resources/EasyLogging/config_with_empty_log_level.json";
        private const string ConfigWithoutLogPathFilePath = "../../../Resources/EasyLogging/config_without_log_path.json";
        private const string ConfigWithWrongLogLevelFilePath = "../../../Resources/EasyLogging/config_with_unknown_log_level.json";

        [Test]
        public void TestThatParsesConfigFile()
        {
            // arrange
            var parser = new EasyLoggingConfigParser();
            
            // act
            var config = parser.Parse(ConfigFilePath);
            
            // assert
            Assert.IsNotNull(config);
            Assert.IsNotNull(config.CommonProps);
            Assert.AreEqual("info", config.CommonProps.LogLevel);
            Assert.AreEqual("./test-logs/log_file.log", config.CommonProps.LogPath);
        }

        [Test]
        public void TestThatReturnsNullIfFileDoesNotExist(
            [Values(null, "", NotExistingFilePath)] string notExistingFilePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();
            
            // act
            var config = parser.Parse(NotExistingFilePath);
            
            // assert
            Assert.IsNull(config);
        }

        [Test]
        [TestCase(EmptyConfigFilePath)]
        [TestCase(ConfigWithoutLogLevelFilePath)]
        [TestCase(ConfigWithNullLogLevelFilePath)]
        [TestCase(ConfigWithEmptyLogLevelFilePath)]
        [TestCase(ConfigWithoutLogPathFilePath)]
        [TestCase(ConfigWithWrongLogLevelFilePath)]
        public void TestThatReturnsNullIfMissingOrInvalidRequiredFields(string filePath)
        {
            // arrange
            var parser = new EasyLoggingConfigParser();
            
            // act
            var config = parser.Parse(ConfigWithoutLogLevelFilePath);
            
            // assert
            Assert.IsNull(config);
        }
    }
}
