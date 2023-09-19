/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
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

        private const string LogPath = "/some-logs-path/some-folder";
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
        
        [ThreadStatic]
        private static Mock<EasyLoggingConfigProvider> t_easyLoggingProvider;
        
        [ThreadStatic]
        private static Mock<EasyLoggerManager> t_easyLoggerManager;
        
        [ThreadStatic]
        private static Mock<DirectoryOperations> t_directoryOperations;

        [ThreadStatic]
        private static EasyLoggingStarter t_easyLoggerStarter;
        
        [SetUp]
        public void BeforeEach()
        {
            t_easyLoggingProvider = new Mock<EasyLoggingConfigProvider>();
            t_easyLoggerManager = new Mock<EasyLoggerManager>();
            t_directoryOperations = new Mock<DirectoryOperations>();
            t_easyLoggerStarter = new EasyLoggingStarter(t_easyLoggingProvider.Object, t_easyLoggerManager.Object, t_directoryOperations.Object);
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
