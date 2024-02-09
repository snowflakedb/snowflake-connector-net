/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Snowflake.Data.Configuration;
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
        public void TestThatParsesConfigFile()
        {
            // arrange
            var parser = new EasyLoggingConfigParser();
            var configFilePath = CreateConfigTempFile(s_workingDirectory, Config(LogLevel, LogPath));

            // act
            var config = parser.Parse(configFilePath);

            // assert
            Assert.IsNotNull(config);
            Assert.IsNotNull(config.CommonProps);
            Assert.AreEqual(LogLevel, config.CommonProps.LogLevel);
            Assert.AreEqual(LogPath, config.CommonProps.LogPath);
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
            Assert.AreEqual("Finding easy logging configuration failed", thrown.Message);
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
            Assert.IsTrue(thrown.Message == "Parsing easy logging configuration failed");
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
