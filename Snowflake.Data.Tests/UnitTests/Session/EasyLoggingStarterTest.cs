/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
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

        [ThreadStatic]
        private static Mock<EasyLoggingConfigProvider> t_easyLoggingProvider;
        
        [ThreadStatic]
        private static Mock<EasyLoggerManager> t_easyLoggerManager;
        
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
            t_directoryOperations = new Mock<DirectoryOperations>();
            t_environmentOperations = new Mock<EnvironmentOperations>();
            t_easyLoggerStarter = new EasyLoggingStarter(t_easyLoggingProvider.Object, t_easyLoggerManager.Object, t_directoryOperations.Object, t_environmentOperations.Object);
        }

        [Test]
        public void TestThatCreatedDirectoryPermissionsFollowUmask()
        {
            // Note: To test with a different value than the default umask, it will have to be set before running this test
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // arrange
                t_easyLoggingProvider
                    .Setup(provider => provider.ProvideConfig(ConfigPath))
                    .Returns(s_configWithInfoLevel);
                t_directoryOperations
                    .Setup(provider => provider.Exists(ConfigPath))
                    .Returns(Directory.Exists(ConfigPath));
                t_directoryOperations
                    .Setup(provider => provider.CreateDirectory(s_expectedLogPath))
                    .Returns(Directory.CreateDirectory(s_expectedLogPath));

                // act
                t_easyLoggerStarter.Init(ConfigPath);
                var umask = EasyLoggerUtil.AllPermissions - int.Parse(EasyLoggerUtil.CallBash("umask"));
                string commandParameters = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "-c '%a'" : "-f %A";
                var dirPermissions = EasyLoggerUtil.CallBash($"stat {commandParameters} {s_expectedLogPath}");

                // assert
                Assert.IsTrue(umask >= int.Parse(dirPermissions));

                // cleanup
                Directory.Delete(s_expectedLogPath);
            }
        }

        [Test]
        public void TestThatThrowsErrorWhenLogPathAndHomeDirectoryIsNotSet()
        {
            // arrange
            t_easyLoggingProvider
                .Setup(provider => provider.ProvideConfig(ConfigPath))
                .Returns(s_configWithNoLogPath);
            t_environmentOperations
                .Setup(provider => provider.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns("");

            // act
            var thrown = Assert.Throws<Exception>(() => t_easyLoggerStarter.Init(ConfigPath));

            // assert
            Assert.IsNotNull(thrown);
            Assert.AreEqual(thrown.Message, "No log path found for easy logging. Home directory is not configured and log path is not provided");
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
            t_directoryOperations.Verify(d => d.CreateDirectory(s_expectedLogPath), Times.Once);
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
            t_directoryOperations.Verify(d => d.CreateDirectory(s_expectedLogPath), Times.Once);
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
            t_directoryOperations.Verify(d => d.CreateDirectory(s_expectedLogPath), Times.Once);
            t_easyLoggerManager.Verify(manager => manager.ReconfigureEasyLogging(EasyLoggingLogLevel.Error, s_expectedLogPath), Times.Once);

            // act
            t_easyLoggerStarter.Init(ConfigPath);
            
            // assert
            t_easyLoggerManager.Verify(manager => manager.ReconfigureEasyLogging(EasyLoggingLogLevel.Info, s_expectedLogPath), Times.Once);
            t_easyLoggerManager.VerifyNoOtherCalls();
        }
    }
}
