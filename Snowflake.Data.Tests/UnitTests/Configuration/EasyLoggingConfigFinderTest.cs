using System;
using System.IO;
using System.Runtime.InteropServices;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    [TestFixture]
    public class EasyLoggingConfigFinderTest
    {
        private const string ConfigFilePath = "file_config.json";
            
        [Test]
        public void TestThatTakesFilePathFromTheInput()
        {
            // arrange
            var finder = new EasyLoggingConfigFinder();
            
            // act
            var filePath = finder.FindConfigFilePath(ConfigFilePath);
            
            // assert
            Assert.AreEqual(ConfigFilePath, filePath);
        }

        [Test]
        public void TestThatDoesNotExecuteOtherChecksWhenValueProvidedAsInputParameter()
        {
            // arrange
            var mock = new Mock<EasyLoggingConfigFinder> { CallBase = true };
            
            // act
            var filePath = mock.Object.FindConfigFilePath(ConfigFilePath);
            
            // assert
            Assert.AreEqual(ConfigFilePath, filePath);
            mock.Verify(finder => finder.GetFilePathEnvironmentVariable(), Times.Never);
            mock.Verify(finder => finder.GetFilePathFromDriverLocation(), Times.Never);
            mock.Verify(finder => finder.GetFilePathFromHomeDirectory(), Times.Never);
            mock.Verify(finder => finder.GetFilePathFromTempDirectory(), Times.Never);
        }

        [Test]
        public void TestThatTakesFilePathFromEnvironmentVariableIfInputNotPresent(
            [Values(null, "")] string inputFilePath)
        {
            // arrange
            var mock = new Mock<EasyLoggingConfigFinder> { CallBase = true };
            mock.Setup(finder => finder.GetFilePathEnvironmentVariable())
                .Returns(ConfigFilePath);
            
            // act
            var filePath = mock.Object.FindConfigFilePath(inputFilePath);
            
            // assert
            Assert.AreEqual(ConfigFilePath, filePath);
            mock.Verify(finder => finder.GetFilePathEnvironmentVariable(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromDriverLocation(), Times.Never);
            mock.Verify(finder => finder.GetFilePathFromHomeDirectory(), Times.Never);
            mock.Verify(finder => finder.GetFilePathFromTempDirectory(), Times.Never);            
        }
        
        [Test]
        public void TestThatTakesFilePathFromDriverLocationWhenNoInputParameterNorEnvironmentVariable()
        {
            // arrange
            var mock = new Mock<EasyLoggingConfigFinder> { CallBase = true };
            mock.Setup(finder => finder.GetFilePathEnvironmentVariable())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromDriverLocation())
                .Returns(ConfigFilePath);
            
            // act
            var filePath = mock.Object.FindConfigFilePath(null);
            
            // assert
            Assert.AreEqual(ConfigFilePath, filePath);
            mock.Verify(finder => finder.GetFilePathEnvironmentVariable(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromDriverLocation(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromHomeDirectory(), Times.Never);
            mock.Verify(finder => finder.GetFilePathFromTempDirectory(), Times.Never);            
        }
        
        [Test]
        public void TestThatTakesFilePathFromHomeLocationWhenNoInputParamEnvironmentVarNorDriverLocation()
        {
            // arrange
            var mock = new Mock<EasyLoggingConfigFinder> { CallBase = true };
            mock.Setup(finder => finder.GetFilePathEnvironmentVariable())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromDriverLocation())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromHomeDirectory())
                .Returns(ConfigFilePath);
            
            // act
            var filePath = mock.Object.FindConfigFilePath(null);
            
            // assert
            Assert.AreEqual(ConfigFilePath, filePath);
            mock.Verify(finder => finder.GetFilePathEnvironmentVariable(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromDriverLocation(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromHomeDirectory(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromTempDirectory(), Times.Never);            
        }

        [Test]
        public void TestThatTakesFilePathFromTempDirectoryWhenNoOtherWaysPossible()
        {
            // arrange
            var mock = new Mock<EasyLoggingConfigFinder> { CallBase = true };
            mock.Setup(finder => finder.GetFilePathEnvironmentVariable())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromDriverLocation())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromHomeDirectory())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromTempDirectory())
                .Returns(ConfigFilePath);
            
            // act
            var filePath = mock.Object.FindConfigFilePath(null);
            
            // assert
            Assert.AreEqual(ConfigFilePath, filePath);
            mock.Verify(finder => finder.GetFilePathEnvironmentVariable(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromDriverLocation(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromHomeDirectory(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromTempDirectory(), Times.Once);  
        }

        [Test]
        public void TestThatReturnsNullIfFilePathCannotBeObtained()
        {
            // arrange
            var mock = new Mock<EasyLoggingConfigFinder> { CallBase = true };
            mock.Setup(finder => finder.GetFilePathEnvironmentVariable())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromDriverLocation())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromHomeDirectory())
                .Returns((string) null);
            mock.Setup(finder => finder.GetFilePathFromTempDirectory())
                .Returns((string) null);
            
            // act
            var filePath = mock.Object.FindConfigFilePath(null);
            
            // assert
            Assert.IsNull(filePath);
            mock.Verify(finder => finder.GetFilePathEnvironmentVariable(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromDriverLocation(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromHomeDirectory(), Times.Once);
            mock.Verify(finder => finder.GetFilePathFromTempDirectory(), Times.Once);  
        }

        [Test]
        public void TestThatGetsFilePathFromEnvironmentVariable()
        {
            // arrange
            var finder = new EasyLoggingConfigFinder();
            var environmentVariableAtTheBeginning = Environment.GetEnvironmentVariable(EasyLoggingConfigFinder.ClientConfigEnvironmentName);
            var expectedFilePath = string.IsNullOrEmpty(environmentVariableAtTheBeginning) ? null : environmentVariableAtTheBeginning;
            
            // act
            var filePath = finder.GetFilePathEnvironmentVariable();
            
            // assert
            Assert.AreEqual(expectedFilePath, filePath);
            
            // arrange
            var filePathToSet = "/Users/dotnetUser/config.json";
            Environment.SetEnvironmentVariable(EasyLoggingConfigFinder.ClientConfigEnvironmentName, filePathToSet);
            
            // act
            var filePathWhenEnvIsSet = finder.GetFilePathEnvironmentVariable();
            
            // assert
            Assert.AreEqual(filePathToSet, filePathWhenEnvIsSet);
            
            // cleanup
            if (environmentVariableAtTheBeginning != filePathToSet)
            {
                Environment.SetEnvironmentVariable(EasyLoggingConfigFinder.ClientConfigEnvironmentName, environmentVariableAtTheBeginning);
            }
        }
        
        
        [Test]
        public void TestThatFindsFileInTempDirectory()
        {
            // arrange
            var finder = new EasyLoggingConfigFinder();
            var tempFilePath = Path.Combine(Path.GetTempPath(), EasyLoggingConfigFinder.ClientConfigFileName);
            var fileExistedAtTheBeginning = File.Exists(tempFilePath);
            var expectedFilePath = fileExistedAtTheBeginning ? tempFilePath : null;
            
            // act
            var filePath = finder.GetFilePathFromTempDirectory();
            
            // assert
            Assert.AreEqual(expectedFilePath, filePath);
            
            // arrange
            CreateEmptyFileIfNotExists(tempFilePath);
            
            // act
            var filePathWhenFileExists = finder.GetFilePathFromTempDirectory();
            
            // assert
            Assert.AreEqual(tempFilePath, filePathWhenFileExists);
            
            // cleanup
            if (!fileExistedAtTheBeginning)
            {
                File.Delete(tempFilePath);
            }
        }
        
        [Test]
        public void TestThatFindsFileInDriverDirectory()
        {
            // arrange
            var finder = new EasyLoggingConfigFinder();
            var driverFilePath = Path.Combine(".", EasyLoggingConfigFinder.ClientConfigFileName);
            var fileExistedAtTheBeginning = File.Exists(driverFilePath);
            var expectedFilePath = fileExistedAtTheBeginning ? driverFilePath : null;
            
            // act
            var filePath = finder.GetFilePathFromDriverLocation();
            
            // assert
            Assert.AreEqual(expectedFilePath, filePath);
            
            // arrange
            CreateEmptyFileIfNotExists(driverFilePath);
            
            // act
            var filePathWhenFileExists = finder.GetFilePathFromDriverLocation();
            
            // assert
            Assert.AreEqual(driverFilePath, filePathWhenFileExists);
            
            // cleanup
            if (!fileExistedAtTheBeginning)
            {
                File.Delete(driverFilePath);
            }
        }
        
        [Test]
        public void TestThatFindsFileInHomeDirectory()
        {
            // arrange
            var finder = new EasyLoggingConfigFinder();
            var homeDirectory = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? Environment.ExpandEnvironmentVariables(EasyLoggingConfigFinder.WindowsHomePathExtractionTemplate)
                : Environment.GetEnvironmentVariable(EasyLoggingConfigFinder.UnixHomeEnvName);
            var homeFilePath = Path.Combine(homeDirectory, EasyLoggingConfigFinder.ClientConfigFileName);
            var fileExistedAtTheBeginning = File.Exists(homeFilePath);
            var expectedFilePath = fileExistedAtTheBeginning ? homeFilePath : null;
            
            // act
            var filePath = finder.GetFilePathFromHomeDirectory();
            
            // assert
            Assert.AreEqual(expectedFilePath, filePath);
            
            // arrange
            CreateEmptyFileIfNotExists(homeFilePath);
            
            // act
            var filePathWhenFileExists = finder.GetFilePathFromHomeDirectory();
            
            // assert
            Assert.AreEqual(homeFilePath, filePathWhenFileExists);
            
            // cleanup
            if (!fileExistedAtTheBeginning)
            {
                File.Delete(homeFilePath);
            }
        }

        private void CreateEmptyFileIfNotExists(string filePath)
        {
            using (var streamWriter = File.AppendText(filePath));
        }
    }
}
