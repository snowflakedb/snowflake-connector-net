/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    [TestFixture]
    public class EasyLoggingConfigFinderTest
    {
        private const string InputConfigFilePath = "input_config.json";
        private const string EnvironmentalConfigFilePath = "environmental_config.json";
        private const string HomeDirectory = "/home/user";
        private const string DriverDirectory = ".";
        private static readonly string s_driverConfigFilePath = Path.Combine(DriverDirectory, EasyLoggingConfigFinder.ClientConfigFileName);
        private static readonly string s_homeConfigFilePath = Path.Combine(HomeDirectory, EasyLoggingConfigFinder.ClientConfigFileName);

        [ThreadStatic]
        private static Mock<FileOperations> t_fileOperations;

        [ThreadStatic]
        private static Mock<UnixOperations> t_unixOperations;

        [ThreadStatic]
        private static Mock<EnvironmentOperations> t_environmentOperations;

        [ThreadStatic]
        private static EasyLoggingConfigFinder t_finder;

        [SetUp]
        public void Setup()
        {
            t_fileOperations = new Mock<FileOperations>();
            t_unixOperations = new Mock<UnixOperations>();
            t_environmentOperations = new Mock<EnvironmentOperations>();
            t_finder = new EasyLoggingConfigFinder(t_fileOperations.Object, t_unixOperations.Object, t_environmentOperations.Object);
            MockHomeDirectory();
            MockExecutionDirectory();
        }

        [Test]
        public void TestThatTakesFilePathFromTheInput()
        {
            // arrange
            MockFileFromEnvironmentalVariable();
            MockFileOnDriverPath();
            MockFileOnHomePath();

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
            MockFileFromEnvironmentalVariable();
            MockFileOnDriverPath();
            MockFileOnHomePath();

            // act
            var filePath = t_finder.FindConfigFilePath(inputFilePath);

            // assert
            Assert.AreEqual(EnvironmentalConfigFilePath, filePath);
        }

        [Test]
        public void TestThatTakesFilePathFromDriverLocationWhenNoInputParameterNorEnvironmentVariable()
        {
            // arrange
            MockFileOnDriverPath();
            MockFileOnHomePath();

            // act
            var filePath = t_finder.FindConfigFilePath(null);

            // assert
            Assert.AreEqual(s_driverConfigFilePath, filePath);
        }

        [Test]
        public void TestThatTakesFilePathFromHomeLocationWhenNoInputParamEnvironmentVarNorDriverLocation()
        {
            // arrange
            MockFileOnHomePath();

            // act
            var filePath = t_finder.FindConfigFilePath(null);

            // assert
            Assert.AreEqual(s_homeConfigFilePath, filePath);
        }

        [Test]
        public void TestThatTakesFilePathFromHomeDirectoryWhenNoOtherWaysPossible()
        {
            // arrange
            MockFileOnHomePath();

            // act
            var filePath = t_finder.FindConfigFilePath(null);

            // assert
            Assert.AreEqual(s_homeConfigFilePath, filePath);
        }

        [Test]
        public void TestThatConfigFileIsNotUsedIfOthersCanModifyTheConfigFile()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            // arrange
            MockFileOnHomePath();
            MockHasFlagReturnsTrue();

            // act
            var thrown = Assert.Throws<Exception>(() => t_finder.FindConfigFilePath(null));

            // assert
            Assert.IsNotNull(thrown);
            Assert.AreEqual(thrown.Message, $"Error due to other users having permission to modify the config file: {s_homeConfigFilePath}");
        }

        [Test]
        public void TestThatReturnsNullIfNoWayOfGettingTheFile()
        {
            // act
            var filePath = t_finder.FindConfigFilePath(null);

            // assert
            Assert.IsNull(filePath);
        }

        [Test]
        public void TestThatDoesNotFailWhenSearchForOneOfDirectoriesFails()
        {
            // arrange
            MockHomeDirectoryFails();

            // act
            var filePath = t_finder.FindConfigFilePath(null);

            // assert
            Assert.IsNull(filePath);
            t_environmentOperations.Verify(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile), Times.Once);
        }

        [Test]
        public void TestThatDoesNotFailWhenHomeDirectoryReturnsNull()
        {
            // arrange
            MockHomeDirectoryReturnsNull();

            // act
            var filePath = t_finder.FindConfigFilePath(null);

            // assert
            Assert.IsNull(filePath);
            t_environmentOperations.Verify(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile), Times.Once);
        }

        [Test]
        public void TestThatDoesNotFailWhenHomeDirectoryDoesNotExist()
        {
            // arrange
            MockFileOnHomePath();
            MockFileOnHomePathDoesNotExist();

            // act
            var filePath = t_finder.FindConfigFilePath(null);

            // assert
            Assert.IsNull(filePath);
            t_environmentOperations.Verify(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile), Times.Once);
        }

        private static void MockHasFlagReturnsTrue()
        {
            t_unixOperations
                .Setup(f => f.CheckFileHasAnyOfPermissions(s_homeConfigFilePath,
                    It.Is<FileAccessPermissions>(p =>  p.Equals(FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite))))
                .Returns(true);
        }

        private static void MockHomeDirectory()
        {
            t_environmentOperations
                .Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns(HomeDirectory);
        }

        private static void MockHomeDirectoryFails()
        {
            t_environmentOperations
                .Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Throws(() => new Exception("No home directory"));
        }

        private static void MockExecutionDirectory()
        {
            t_environmentOperations
                .Setup(e => e.GetExecutionDirectory())
                .Returns(DriverDirectory);
        }

        private static void MockFileOnHomePathDoesNotExist()
        {
            t_fileOperations
                .Setup(f => f.Exists(s_homeConfigFilePath))
                .Returns(false);
        }

        private static void MockHomeDirectoryReturnsNull()
        {
            t_environmentOperations
                .Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns((string) null);
        }

        private static void MockFileFromEnvironmentalVariable()
        {
            t_environmentOperations
                .Setup(e => e.GetEnvironmentVariable(EasyLoggingConfigFinder.ClientConfigEnvironmentName, null))
                .Returns(EnvironmentalConfigFilePath);
        }

        private static void MockFileOnDriverPath()
        {
            t_fileOperations
                .Setup(f => f.Exists(s_driverConfigFilePath))
                .Returns(true);
        }

        private static void MockFileOnHomePath()
        {
            t_fileOperations
                .Setup(f => f.Exists(s_homeConfigFilePath))
                .Returns(true);
        }
    }
}
