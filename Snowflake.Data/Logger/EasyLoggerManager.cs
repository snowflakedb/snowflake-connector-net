/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Linq;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Log
{
    internal class EasyLoggerManager
    {
        public static readonly EasyLoggerManager Instance = new EasyLoggerManager();

        private readonly object _lockForExclusiveConfigure = new object();

        internal const string AppenderPrefix = "SFEasyLogging";

        private readonly EasyLoggingLevelMapper _levelMapper = EasyLoggingLevelMapper.Instance;

        public virtual void ReconfigureEasyLogging(EasyLoggingLogLevel easyLoggingLogLevel, string logsPath)
        {
            var sfLoggerLevel = _levelMapper.ToLoggingEventLevel(easyLoggingLogLevel);
            lock (_lockForExclusiveConfigure)
            {
                var rootLogger = SFLogRepository.GetRootLogger();
                rootLogger.SetLevel(sfLoggerLevel);
                var appender = string.Equals(logsPath, "STDOUT", StringComparison.OrdinalIgnoreCase)
                    ? AddConsoleAppender(rootLogger)
                    : AddRollingFileAppender(rootLogger, logsPath);
                RemoveOtherEasyLoggingAppenders(rootLogger, appender);
            }
        }

        internal void ResetEasyLogging(EasyLoggingLogLevel easyLoggingLogLevel)
        {
            var sfLoggerLevel = _levelMapper.ToLoggingEventLevel(easyLoggingLogLevel);
            lock (_lockForExclusiveConfigure)
            {
                var rootLogger = SFLogRepository.GetRootLogger();
                rootLogger.SetLevel(sfLoggerLevel);
                RemoveOtherEasyLoggingAppenders(rootLogger, null);
            }
        }

        internal static bool HasEasyLoggingAppender()
        {
            var rootLogger = SFLogRepository.GetRootLogger();
            return rootLogger.GetAppenders().ToArray().Any(IsEasyLoggingAppender);
        }

        private static void RemoveOtherEasyLoggingAppenders(SFLogger logger, SFAppender appender)
        {
            var existingAppenders = logger.GetAppenders().ToArray();
            foreach (var existingAppender in existingAppenders)
            {
                if (IsEasyLoggingAppender(existingAppender) && existingAppender != appender)
                {
                    logger.RemoveAppender(existingAppender);
                }
            }
        }

        private static SFAppender AddRollingFileAppender(SFLogger logger,
            string directoryPath)
        {
            var patternLayout = PatternLayout();
            var randomFileName = $"snowflake_dotnet_{Path.GetRandomFileName()}";
            var logFileName = randomFileName.Substring(0, randomFileName.Length - 4) + ".log";
            var appender = new SFRollingFileAppender
            {
                _patternLayout = patternLayout,
                _logFilePath = Path.Combine(directoryPath, logFileName),
                _name = $"{AppenderPrefix}RollingFileAppender",
                _maximumFileSizeInBytes = 1000000000, // "1GB"
                _maxSizeRollBackups = 2,
            };
            appender.ActivateOptions();
            logger.AddAppender(appender);
            return appender;
        }

        private static bool IsEasyLoggingAppender(SFAppender appender)
        {
            if (appender.GetType() == typeof(SFConsoleAppender))
            {
                var consoleAppender = (SFConsoleAppender)appender;
                return consoleAppender._name != null && consoleAppender._name.StartsWith(AppenderPrefix);
            }

            if (appender.GetType() == typeof(SFRollingFileAppender))
            {
                var rollingFileAppender = (SFRollingFileAppender)appender;
                return rollingFileAppender._name != null && rollingFileAppender._name.StartsWith(AppenderPrefix);
            }

            return false;
        }

        private static SFAppender AddConsoleAppender(SFLogger logger)
        {
            var patternLayout = PatternLayout();
            var appender = new SFConsoleAppender()
            {
                _patternLayout = patternLayout,
                _name = $"{AppenderPrefix}ConsoleAppender"
            };
            logger.AddAppender(appender);
            return appender;
        }

        internal static PatternLayout PatternLayout()
        {
            var patternLayout = new PatternLayout
            {
                _conversionPattern = "[%date] [%t] [%-5level] [%logger] %message%newline"
            };
            return patternLayout;
        }
    }
}
