using System;
using System.IO;
using System.Linq;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Log
{
    internal class EasyLoggerManager
    {
        public static readonly EasyLoggerManager Instance = new EasyLoggerManager();

        private readonly object _lockForExclusiveConfigure = new object();

        private readonly EasyLoggingLevelMapper _levelMapper = EasyLoggingLevelMapper.Instance;

        public virtual void ReconfigureEasyLogging(EasyLoggingLogLevel easyLoggingLogLevel, string logsPath)
        {
            var sfLoggerLevel = _levelMapper.ToLoggingEventLevel(easyLoggingLogLevel);
            lock (_lockForExclusiveConfigure)
            {
                SFLoggerImpl.SetLevel(sfLoggerLevel);
                var appender = IsStdout(logsPath)
                    ? AddConsoleAppender()
                    : AddRollingFileAppender(logsPath);
                RemoveOtherEasyLoggingAppenders(appender);
                appender.ActivateOptions();
                SFLoggerImpl.s_appenders.Add(appender);
            }
        }

        internal static bool IsStdout(string logsPath)
        {
            return string.Equals(logsPath, "STDOUT", StringComparison.OrdinalIgnoreCase);
        }

        internal void ResetEasyLogging(EasyLoggingLogLevel easyLoggingLogLevel)
        {
            var sfLoggerLevel = _levelMapper.ToLoggingEventLevel(easyLoggingLogLevel);
            lock (_lockForExclusiveConfigure)
            {
                SFLoggerImpl.SetLevel(sfLoggerLevel);
                if (easyLoggingLogLevel == EasyLoggingLogLevel.Off)
                    RemoveOtherEasyLoggingAppenders(null);
            }
        }

        internal static bool HasEasyLoggingAppender()
        {
            return SFLoggerImpl.s_appenders.Any();
        }

        private static void RemoveOtherEasyLoggingAppenders(SFAppender appender)
        {
            foreach (var existingAppender in SFLoggerImpl.s_appenders.ToArray())
            {
                if (existingAppender != appender)
                {
                    SFLoggerImpl.s_appenders.Remove(existingAppender);
                }
            }
        }

        private static SFAppender AddRollingFileAppender(string directoryPath)
        {
            var patternLayout = PatternLayout();
            var randomFileName = $"snowflake_dotnet_{Path.GetRandomFileName()}";
            var logFileName = randomFileName.Substring(0, randomFileName.Length - 4) + ".log";
            var appender = new SFRollingFileAppender(FileOperations.Instance, UnixOperations.Instance, DirectoryOperations.Instance)
            {
                PatternLayout = patternLayout,
                LogFilePath = Path.Combine(directoryPath, logFileName),
                MaximumFileSizeInBytes = 1000000000, // "1GB"
                MaxSizeRollBackups = 2,
            };

            return appender;
        }

        internal static SFAppender AddConsoleAppender()
        {
            var patternLayout = PatternLayout();
            var appender = new SFConsoleAppender()
            {
                PatternLayout = patternLayout,
            };
            return appender;
        }

        internal static PatternLayout PatternLayout()
        {
            var patternLayout = new PatternLayout
            {
                ConversionPattern = "[%date] [%t] [%-5level] [%logger] %message%newline"
            };
            return patternLayout;
        }
    }
}
