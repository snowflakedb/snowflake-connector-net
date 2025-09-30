using Moq;
using NUnit.Framework;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    [TestFixture]
    public class EasyLoggingConfigProviderTest
    {
        private const string FilePathFromConnectionString = "/Users/dotnet/config.json";
        private const string FilePathToUse = "/home/config.json";

        [Test]
        public void TestThatProvidesConfiguration()
        {
            // arrange
            var configFinder = new Mock<EasyLoggingConfigFinder>();
            var configParser = new Mock<EasyLoggingConfigParser>();
            var configProvider = new EasyLoggingConfigProvider(configFinder.Object, configParser.Object);
            var config = new ClientConfig();
            configFinder
                .Setup(finder => finder.FindConfigFilePath(FilePathFromConnectionString))
                .Returns(FilePathToUse);
            configParser
                .Setup(parser => parser.Parse(FilePathToUse))
                .Returns(config);

            // act
            var result = configProvider.ProvideConfig(FilePathFromConnectionString);

            // assert
            Assert.AreSame(config, result);
        }

        [Test]
        public void TestThatReturnsNullWhenNoConfigurationFound()
        {
            // arrange
            var configFinder = new Mock<EasyLoggingConfigFinder>();
            var configParser = new Mock<EasyLoggingConfigParser>();
            var configProvider = new EasyLoggingConfigProvider(configFinder.Object, configParser.Object);
            configFinder
                .Setup(finder => finder.FindConfigFilePath(FilePathFromConnectionString))
                .Returns((string)null);

            // act
            var result = configProvider.ProvideConfig(FilePathFromConnectionString);

            // assert
            Assert.IsNull(result);
            configParser.Verify(parser => parser.Parse(It.IsAny<string>()), Times.Never);
        }
    }
}
