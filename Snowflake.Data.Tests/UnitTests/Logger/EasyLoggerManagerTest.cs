/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [TestFixture, NonParallelizable]
    public class EasyLoggerManagerTest
    {
        
        private const string InfoMessage = "Easy logging Info message";
        private const string DebugMessage = "Easy logging Debug message";
        private const string WarnMessage = "Easy logging Warn message";
        private const string ErrorMessage = "Easy logging Error message";
        private const string FatalMessage = "Easy logging Fatal message";
        private static readonly string s_logsDirectory = Path.GetTempPath();

        [ThreadStatic]
        private static string t_directoryLogPath;
        
        [OneTimeTearDown]
        public static void CleanUp()
        {
            RemoveEasyLoggingLogFiles();
        }

        [SetUp]
        public void BeforeEach()
        {
            t_directoryLogPath = RandomLogsDirectoryPath();
        }

        [TearDown]
        public void AfterEach()
        {
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, t_directoryLogPath);
        }
        
        [Test]
        public void TestThatChangesLogLevel()
        {
            // arrange
            var logger = SFLoggerFactory.GetLogger<SFBlockingChunkDownloaderV3>();
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Warn, t_directoryLogPath);
            
            // assert
            Assert.IsFalse(logger.IsDebugEnabled());
            Assert.IsFalse(logger.IsInfoEnabled());
            Assert.IsTrue(logger.IsWarnEnabled());
            Assert.IsTrue(logger.IsErrorEnabled());
            Assert.IsTrue(logger.IsFatalEnabled());

            // act
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Debug, t_directoryLogPath);

            // assert
            Assert.IsTrue(logger.IsDebugEnabled());
            Assert.IsTrue(logger.IsInfoEnabled());
            Assert.IsTrue(logger.IsWarnEnabled());
            Assert.IsTrue(logger.IsErrorEnabled());
            Assert.IsTrue(logger.IsFatalEnabled());
        }

        [Test]
        public void TestThatLogsToProperFileWithProperLogLevelOnly()
        {
            // arrange
            var logger = SFLoggerFactory.GetLogger<SFBlockingChunkDownloaderV3>();
            EasyLoggerManager.Instance.ReconfigureEasyLogging(EasyLoggingLogLevel.Info, t_directoryLogPath);
            
            // act
            logger.Debug(DebugMessage);
            logger.Info(InfoMessage);
            logger.Warn(WarnMessage);
            logger.Error(ErrorMessage);
            logger.Fatal(FatalMessage);

            // assert
            var logLines = File.ReadLines(FindLogFilePath(t_directoryLogPath));
            Assert.That(logLines, Has.Exactly(0).Matches<string>(s => s.Contains(DebugMessage)));
            Assert.That(logLines, Has.Exactly(1).Matches<string>(s => s.Contains(InfoMessage)));
            Assert.That(logLines, Has.Exactly(1).Matches<string>(s => s.Contains(WarnMessage)));
            Assert.That(logLines, Has.Exactly(1).Matches<string>(s => s.Contains(ErrorMessage)));
            Assert.That(logLines, Has.Exactly(1).Matches<string>(s => s.Contains(FatalMessage)));
        }
        
        private static string RandomLogsDirectoryPath()
        {
            var randomName = Path.GetRandomFileName();
            return Path.Combine(s_logsDirectory, $"easy_logging_logs_{randomName}", "dotnet");
        }

        private static string FindLogFilePath(string directoryLogPath)
        {
            Assert.IsTrue(Directory.Exists(directoryLogPath));
            var files = Directory.GetFiles(directoryLogPath);
            Assert.AreEqual(1, files.Length);
            return files.First();
        }
        
        private static void RemoveEasyLoggingLogFiles()
        {
            Directory.GetFiles(s_logsDirectory)
                .Where(filePath => filePath.StartsWith(Path.Combine(s_logsDirectory, "easy_logging_logs")))
                .AsParallel()
                .ForAll(filePath => File.Delete(filePath));
        }
    }
}
