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
        private const string InputConfigFilePath = "input_config.json";
        private const string EnvironmentalConfigFilePath = "environmental_config.json";
        private const string HomeDirectory = "/home/user";
        private static readonly string s_driverConfigFilePath = Path.Combine(".", EasyLoggingConfigFinder.ClientConfigFileName);
        private static readonly string s_homeConfigFilePath = Path.Combine(HomeDirectory, EasyLoggingConfigFinder.ClientConfigFileName);
        private static readonly string s_tempConfigFilePath = Path.Combine(Path.GetTempPath(), EasyLoggingConfigFinder.ClientConfigFileName);

        [ThreadStatic]
        private static Mock<FileOperations> t_fileOperations;

        [ThreadStatic]
        private static Mock<EnvironmentOperations> t_environmentOperations;

        [ThreadStatic]
        private static EasyLoggingConfigFinder t_finder;

        [SetUp]
        public void Setup()
        {
            t_fileOperations = new Mock<FileOperations>();
            t_environmentOperations = new Mock<EnvironmentOperations>();
            t_finder = new EasyLoggingConfigFinder(t_fileOperations.Object, t_environmentOperations.Object);
            mockHomeDirectory();
        }
        
        [Test]
        public void TestThatTakesFilePathFromTheInput()
        {
            // arrange
            mockFileFromEnvironmentalVariable();
            mockFileOnDriverPath();
            mockFileOnHomePath();
            mockFileOnTempPath();
            
            // act
            var filePath = t_finder.FindConfigFilePath(InputConfigFilePath);
            
            // assert
            Assert.AreEqual(InputConfigFilePath, filePath);
            t_fileOperations.VerifyNoOtherCalls();
            t_environmentOperations.VerifyNoOtherCalls();
        }

        [Test]
        public void TestThatTakesFilePathFromEnvironmentVariableIfInputNotPresent(
            [Values(null, "")] string inputFilePath)
        {
            // arrange
            mockFileFromEnvironmentalVariable();
            mockFileOnDriverPath();
            mockFileOnHomePath();
            mockFileOnTempPath();
            
            // act
            var filePath = t_finder.FindConfigFilePath(inputFilePath);
            
            // assert
            Assert.AreEqual(EnvironmentalConfigFilePath, filePath);
        }
        
        [Test]
        public void TestThatTakesFilePathFromDriverLocationWhenNoInputParameterNorEnvironmentVariable()
        {
            // arrange
            mockFileOnDriverPath();
            mockFileOnHomePath();
            mockFileOnTempPath();

            // act
            var filePath = t_finder.FindConfigFilePath(null);
            
            // assert
            Assert.AreEqual(s_driverConfigFilePath, filePath);
        }
        
        [Test]
        public void TestThatTakesFilePathFromHomeLocationWhenNoInputParamEnvironmentVarNorDriverLocation()
        {
            // arrange
            mockFileOnHomePath();
            mockFileOnTempPath();
            
            // act
            var filePath = t_finder.FindConfigFilePath(null);
            
            // assert
            Assert.AreEqual(s_homeConfigFilePath, filePath);
        }

        [Test]
        public void TestThatTakesFilePathFromTempDirectoryWhenNoOtherWaysPossible()
        {
            // arrange
            mockFileOnTempPath();
            
            // act
            var filePath = t_finder.FindConfigFilePath(null);
            
            // assert
            Assert.AreEqual(s_tempConfigFilePath, filePath);
        }
        
        [Test]
        public void TestThatReturnsNullIfNoWayOfGettingTheFile()
        {
            // act
            var filePath = t_finder.FindConfigFilePath(null);
            
            // assert
            Assert.IsNull(filePath);
        }

        private static void mockHomeDirectory()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                t_environmentOperations
                    .Setup(e => e.ExpandEnvironmentVariables(EasyLoggingConfigFinder.WindowsHomePathExtractionTemplate))
                    .Returns(HomeDirectory);
            }
            else
            {
                t_environmentOperations
                    .Setup(e => e.GetEnvironmentVariable(EasyLoggingConfigFinder.UnixHomeEnvName))
                    .Returns(HomeDirectory);
            }
        }

        private static void mockFileFromEnvironmentalVariable()
        {
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(EasyLoggingConfigFinder.ClientConfigEnvironmentName))
                .Returns(EnvironmentalConfigFilePath);
        }

        private static void mockFileOnDriverPath()
        {
            t_fileOperations
                .Setup(f => f.Exists(s_driverConfigFilePath))
                .Returns(true);
        }

        private static void mockFileOnHomePath()
        {
            t_fileOperations
                .Setup(f => f.Exists(s_homeConfigFilePath))
                .Returns(true);
        }

        private static void mockFileOnTempPath()
        {
            t_fileOperations
                .Setup(f => f.Exists(s_tempConfigFilePath))
                .Returns(true);            
        }
    }
}
