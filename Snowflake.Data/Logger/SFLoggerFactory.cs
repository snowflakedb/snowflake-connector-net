/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Extensions.Logging;
using System.IO;

namespace Snowflake.Data.Log
{
    public class SFLoggerFactory
    {
        private static bool s_isCustomLoggerEnabled = false;

        private static bool s_isSFLoggerEnabled = true;

        private static ILogger s_customLogger = null;

        public static void DisableSFLogger()
        {
            s_isSFLoggerEnabled = false;
        }

        public static void EnableSFLogger()
        {
            s_isSFLoggerEnabled = true;
        }

        public static void DisableCustomLogger()
        {
            s_isCustomLoggerEnabled = false;
        }

        public static void EnableCustomLogger()
        {
            s_isCustomLoggerEnabled = true;
        }

        public static void UseDefaultLogger()
        {
            s_customLogger = null;
        }

        public static void SetCustomLogger(ILogger customLogger)
        {
            s_customLogger = customLogger;
        }

        internal static SFLogger GetLogger<T>()
        {
            return new SFLoggerPair(GetSFLogger<T>(), GetCustomLogger<T>());
        }

        internal static SFLogger GetSFLogger<T>(bool useFileAppender = true)
        {
            // If true, return the default/specified logger
            if (s_isSFLoggerEnabled)
            {
                var logger = new SFLoggerImpl(typeof(T));
                if (useFileAppender)
                {
                    var fileAppender = new SFRollingFileAppender()
                    {
                        _name = "RollingFileAppender",
                        _logFilePath = Path.Combine(Directory.GetCurrentDirectory(), "test_snowflake_log.log"),
                        _maximumFileSizeInBytes = 1000000000, // "1GB"
                        _maxSizeRollBackups = 0,
                        _patternLayout = EasyLoggerManager.PatternLayout()
                    };
                    logger.AddAppender(fileAppender);
                }
                else
                {
                    var consoleAppender = new SFConsoleAppender()
                    {
                        _name = "ConsoleAppender",
                        _patternLayout = EasyLoggerManager.PatternLayout()
                    };
                    logger.AddAppender(consoleAppender);
                }
                return logger;
            }
            // Else, return the empty logger implementation which outputs nothing
            else
            {
                return new SFLoggerEmptyImpl();
            }
        }

        internal static ILogger GetCustomLogger<T>()
        {
            // If true, return the default/specified logger
            if (s_isCustomLoggerEnabled)
            {
                // If no logger specified, use the default logger: Microsoft's console logger
                if (s_customLogger == null)
                {
                    ILoggerFactory factory = LoggerFactory.Create(
                        builder => builder
                        .AddConsole()
                        .SetMinimumLevel(LogLevel.Trace)
                    );

                    return factory.CreateLogger<T>();
                }
                return s_customLogger;
            }
            // Else, return the empty logger implementation which outputs nothing
            else
            {
                return new ILoggerEmptyImpl();
            }
        }
    }
}
