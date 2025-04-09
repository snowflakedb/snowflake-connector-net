using System;
using System.IO;
using System.Linq;
using log4net;
using log4net.Appender;
using log4net.Layout;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Log
{
    internal class EasyLoggerManager
    {
        public static readonly EasyLoggerManager Instance = new EasyLoggerManager();

        private readonly object _lockForExclusiveConfigure = new object();

        private const string AppenderPrefix = "SFEasyLogging";

        private readonly EasyLoggingLevelMapper _levelMapper = EasyLoggingLevelMapper.Instance;

        public virtual void ReconfigureEasyLogging(EasyLoggingLogLevel easyLoggingLogLevel, string logsPath)
        {
            var log4netLevel = _levelMapper.ToLog4NetLevel(easyLoggingLogLevel);
            lock (_lockForExclusiveConfigure)
            {
                var repository = (log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository();
                var rootLogger = (log4net.Repository.Hierarchy.Logger)repository.GetLogger("Snowflake.Data");
                rootLogger.Level = log4netLevel;
                var appender = IsStdout(logsPath)
                    ? AddConsoleAppender(rootLogger)
                    : AddRollingFileAppender(rootLogger, logsPath);
                RemoveOtherEasyLoggingAppenders(rootLogger, appender);
                repository.RaiseConfigurationChanged(EventArgs.Empty);
            }
        }

        internal static bool IsStdout(string logsPath)
        {
            return string.Equals(logsPath, "STDOUT", StringComparison.OrdinalIgnoreCase);
        }

        internal void ResetEasyLogging(EasyLoggingLogLevel easyLoggingLogLevel)
        {
            var log4netLevel = _levelMapper.ToLog4NetLevel(easyLoggingLogLevel);
            lock (_lockForExclusiveConfigure)
            {
                var repository = (log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository();
                var rootLogger = (log4net.Repository.Hierarchy.Logger)repository.GetLogger("Snowflake.Data");
                rootLogger.Level = log4netLevel;
                RemoveOtherEasyLoggingAppenders(rootLogger, null);
                repository.RaiseConfigurationChanged(EventArgs.Empty);
            }
        }

        internal static bool HasEasyLoggingAppender()
        {
            var repository = (log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository();
            var rootLogger = (log4net.Repository.Hierarchy.Logger)repository.GetLogger("Snowflake.Data");
            return rootLogger.Appenders.ToArray().Any(IsEasyLoggingAppender);
        }

        private static void RemoveOtherEasyLoggingAppenders(log4net.Repository.Hierarchy.Logger logger, IAppender appender)
        {
            var existingAppenders = logger.Appenders.ToArray();
            foreach (var existingAppender in existingAppenders)
            {
                if (IsEasyLoggingAppender(existingAppender) && existingAppender != appender)
                {
                    logger.RemoveAppender(existingAppender);
                }
            }
        }

        private static IAppender AddRollingFileAppender(log4net.Repository.Hierarchy.Logger logger,
            string directoryPath)
        {
            var patternLayout = PatternLayout();
            var randomFileName = $"snowflake_dotnet_{Path.GetRandomFileName()}";
            var logFileName = randomFileName.Substring(0, randomFileName.Length - 4) + ".log";
            var appender = new RollingFileAppender
            {
                Layout = patternLayout,
                AppendToFile = true,
                File = Path.Combine(directoryPath, logFileName),
                Name = $"{AppenderPrefix}RollingFileAppender",
                StaticLogFileName = true,
                RollingStyle = RollingFileAppender.RollingMode.Size,
                MaximumFileSize = "1GB",
                MaxSizeRollBackups = 2,
                PreserveLogFileNameExtension = true,
                LockingModel = new FileAppender.MinimalLock()
            };
            appender.ActivateOptions();
            logger.AddAppender(appender);
            return appender;
        }

        private static bool IsEasyLoggingAppender(IAppender appender)
        {
            if (appender.GetType() == typeof(ConsoleAppender))
            {
                var consoleAppender = (ConsoleAppender)appender;
                return consoleAppender.Name != null && consoleAppender.Name.StartsWith(AppenderPrefix);
            }

            if (appender.GetType() == typeof(RollingFileAppender))
            {
                var rollingFileAppender = (RollingFileAppender)appender;
                return rollingFileAppender.Name != null && rollingFileAppender.Name.StartsWith(AppenderPrefix);
            }

            return false;
        }

        private static IAppender AddConsoleAppender(log4net.Repository.Hierarchy.Logger logger)
        {
            var patternLayout = PatternLayout();
            var appender = new ConsoleAppender()
            {
                Layout = patternLayout,
                Name = $"{AppenderPrefix}ConsoleAppender"
            };
            appender.ActivateOptions();
            logger.AddAppender(appender);
            return appender;
        }

        private static PatternLayout PatternLayout()
        {
            var patternLayout = new PatternLayout
            {
                ConversionPattern = "[%date] [%t] [%-5level] [%logger] %message%newline"
            };
            patternLayout.ActivateOptions();
            return patternLayout;
        }
    }
}
