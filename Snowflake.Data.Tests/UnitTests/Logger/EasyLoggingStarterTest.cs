using System;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class EasyLoggingStarterTest
    {
        private static readonly string HomeDirectory = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        private static readonly string LogPath = Path.Combine(HomeDirectory, "some-logs-path/some-folder");
        private const string ConfigPath = "/some-path/config.json";
        private const string AnotherConfigPath = "/another/path";
        private static readonly string s_expectedLogPath = Path.Combine(LogPath, "dotnet");

        private static readonly ClientConfig s_configWithErrorLevel = new ClientConfig
        {
            CommonProps = new ClientConfigCommonProps
            {
                LogLevel = "Error",
                LogPath = LogPath
            }
        };

        private static readonly ClientConfig s_configWithInfoLevel = new ClientConfig
        {
            CommonProps = new ClientConfigCommonProps
            {
                LogLevel = "Info",
                LogPath = LogPath
            }
        };

        private static readonly ClientConfig s_configWithNoLogPath = new ClientConfig
        {
            CommonProps = new ClientConfigCommonProps
            {
                LogLevel = "Info"
            }
        };

        private static readonly ClientConfig s_configWithStdoutAsLogPath = new ClientConfig
        {
            CommonProps = new ClientConfigCommonProps
            {
                LogLevel = "Info",
                LogPath = "STDOUT"
            }
        };

        [ThreadStatic]
        private static Mock<EasyLoggingConfigProvider> t_easyLoggingProvider;

        [ThreadStatic]
        private static Mock<EasyLoggerManager> t_easyLoggerManager;

        [ThreadStatic]
        private static Mock<UnixOperations> t_unixOperations;

        [ThreadStatic]
        private static Mock<DirectoryOperations> t_directoryOperations;

        [ThreadStatic]
        private static Mock<EnvironmentOperations> t_environmentOperations;

        [ThreadStatic]
        private static EasyLoggingStarter t_easyLoggerStarter;

        [SetUp]
        public void BeforeEach()
        {
            t_easyLoggingProvider = new Mock<EasyLoggingConfigProvider>();
            t_easyLoggerManager = new Mock<EasyLoggerManager>();
            t_unixOperations = new Mock<UnixOperations>();
            t_directoryOperations = new Mock<DirectoryOperations>();
            t_environmentOperations = new Mock<EnvironmentOperations>();
            t_easyLoggerStarter = new EasyLoggingStarter(
                t_easyLoggingProvider.Object,
                t_easyLoggerManager.Object,
                t_unixOperations.Object,
                t_directoryOperations.Object,
                t_environmentOperations.Object);
        }

        [Test]
        public void TestThatThrowsErrorWhenLogPathAndHomeDirectoryIsNotSet()
        {
            // arrange
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(ConfigPath))
                .Returns(s_configWithNoLogPath);
            t_environmentOperations
                .Setup(env => env.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns("");

            // act
            var thrown = Assert.Throws<Exception>(() => t_easyLoggerStarter.Init(ConfigPath));

            // assert
            Assert.IsNotNull(thrown);
            Assert.AreEqual(thrown.Message, "No log path found for easy logging. Home directory is not configured and log path is not provided");
        }

        [Test]
        public void TestThatThrowsErrorWhenLogPathIsNotSetAndHomeDirectoryThrowsAnException()
        {
            // arrange
            var ex = new Exception("No home directory");
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(ConfigPath))
                .Returns(s_configWithNoLogPath);
            t_environmentOperations
                .Setup(env => env.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Throws(() => ex);

            // act
            var thrown = Assert.Throws<Exception>(() => t_easyLoggerStarter.Init(ConfigPath));

            // assert
            Assert.IsNotNull(thrown);
            Assert.AreEqual(thrown.Message, "No log path found for easy logging. Home directory is not configured and log path is not provided");
        }

        [Test]
        public void TestThatDoesNotFailWhenLogDirectoryPermissionIsNot700()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            // arrange
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(ConfigPath))
                .Returns(s_configWithInfoLevel);
            t_directoryOperations
                .Setup(dir => dir.Exists(s_expectedLogPath))
                .Returns(true);
            t_unixOperations
                .Setup(unix => unix.GetDirPermissions(s_expectedLogPath))
                .Returns(FileAccessPermissions.AllPermissions);

            // act
            t_easyLoggerStarter.Init(ConfigPath);

            // assert
            t_unixOperations.Verify(u => u.CreateDirectoryWithPermissions(s_expectedLogPath,
                FileAccessPermissions.UserReadWriteExecute), Times.Never);
        }

        [Test]
        public void TestFailIfDirectoryCreationFails()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            // arrange
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(ConfigPath))
                .Returns(s_configWithErrorLevel);
            t_directoryOperations
                .Setup(d => d.CreateDirectory(s_expectedLogPath))
                .Throws(() => new Exception("Unable to create directory"));
            t_unixOperations
                .Setup(u => u.CreateDirectoryWithPermissions(s_expectedLogPath, FileAccessPermissions.UserReadWriteExecute))
                .Throws(() => new Exception("Unable to create directory"));

            // act
            var thrown = Assert.Throws<Exception>(() => t_easyLoggerStarter.Init(ConfigPath));

            // assert
            Assert.That(thrown.Message, Does.Contain("Failed to create logs directory"));
        }

        [Test]
        public void TestThatConfiguresEasyLoggingOnlyOnceWhenInitializedWithConfigPath()
        {
            // arrange
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(ConfigPath))
                .Returns(s_configWithErrorLevel);
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(null))
                .Returns(s_configWithInfoLevel);
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(AnotherConfigPath))
                .Returns(s_configWithInfoLevel);

            // act
            t_easyLoggerStarter.Init(ConfigPath);

            // assert
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                t_directoryOperations.Verify(d => d.CreateDirectory(s_expectedLogPath), Times.Once);
            }
            else
            {
                t_unixOperations.Verify(u => u.CreateDirectoryWithPermissions(s_expectedLogPath,
                    FileAccessPermissions.UserReadWriteExecute), Times.Once);
            }
            t_easyLoggerManager.Verify(manager => manager.ReconfigureEasyLogging(EasyLoggingLogLevel.Error, s_expectedLogPath), Times.Once);

            // act
            t_easyLoggerStarter.Init(null);
            t_easyLoggerStarter.Init(ConfigPath);
            t_easyLoggerStarter.Init(AnotherConfigPath);

            // assert
            t_easyLoggerManager.VerifyNoOtherCalls();
        }

        [Test]
        public void TestThatConfiguresEasyLoggingOnlyOnceForInitializationsWithoutConfigPath()
        {
            // arrange
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(null))
                .Returns(s_configWithErrorLevel);

            // act
            t_easyLoggerStarter.Init(null);
            t_easyLoggerStarter.Init(null);

            // assert
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                t_directoryOperations.Verify(d => d.CreateDirectory(s_expectedLogPath), Times.Once);
            }
            else
            {
                t_unixOperations.Verify(u => u.CreateDirectoryWithPermissions(s_expectedLogPath,
                    FileAccessPermissions.UserReadWriteExecute), Times.Once);
            }
            t_easyLoggerManager.Verify(manager => manager.ReconfigureEasyLogging(EasyLoggingLogLevel.Error, s_expectedLogPath), Times.Once);
        }

        [Test]
        public void TestThatReconfiguresEasyLoggingWithConfigPathIfNotGivenForTheFirstTime()
        {
            // arrange
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(null))
                .Returns(s_configWithErrorLevel);
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(ConfigPath))
                .Returns(s_configWithInfoLevel);

            // act
            t_easyLoggerStarter.Init(null);

            // assert
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                t_directoryOperations.Verify(d => d.CreateDirectory(s_expectedLogPath), Times.Once);
            }
            else
            {
                t_unixOperations.Verify(u => u.CreateDirectoryWithPermissions(s_expectedLogPath,
                    FileAccessPermissions.UserReadWriteExecute), Times.Once);
            }
            t_easyLoggerManager.Verify(manager => manager.ReconfigureEasyLogging(EasyLoggingLogLevel.Error, s_expectedLogPath), Times.Once);

            // act
            t_easyLoggerStarter.Init(ConfigPath);

            // assert
            t_easyLoggerManager.Verify(manager => manager.ReconfigureEasyLogging(EasyLoggingLogLevel.Info, s_expectedLogPath), Times.Once);
            t_easyLoggerManager.VerifyNoOtherCalls();
        }

        [Test]
        public void TestConfigureStdout()
        {
            // arrange
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(null))
                .Returns(s_configWithStdoutAsLogPath);

            // act
            t_easyLoggerStarter.Init(null);

            // assert
            t_easyLoggerManager.Verify(manager => manager.ReconfigureEasyLogging(EasyLoggingLogLevel.Info, "STDOUT"), Times.Once);
        }
    }
}
