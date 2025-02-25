/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Log
{
    internal class SFLoggerFactory
    {
        internal static bool s_isCustomLoggerEnabled = false;

        internal static bool s_isSFLoggerEnabled = false;

        internal static bool s_useDefaultSFLogger = true;

        internal static ILogger s_customLogger = new LoggerEmptyImpl();

        internal static void UseEmptySFLogger()
        {
            s_useDefaultSFLogger = false;
        }

        internal static void UseDefaultSFLogger()
        {
            s_useDefaultSFLogger = true;
        }

        internal static SFLogger GetLogger<T>()
        {
            return new SFLoggerPair(GetSFLogger<T>(), GetCustomLogger<T>());
        }

        internal static SFLogger GetSFLogger<T>(bool useConsoleAppender = false)
        {
            // If true, return the default/specified logger
            if (s_useDefaultSFLogger)
            {
                var logger = new SFLoggerImpl(typeof(T));
                if (!s_isSFLoggerEnabled)
                {
                    SFLoggerImpl.SetLevel(LoggingEvent.OFF); // Logger is disabled by default and can be enabled by the EasyLogging feature
                }
                if(useConsoleAppender)
                {
                    EasyLoggerManager.AddConsoleAppender();
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
                return new LoggerEmptyImpl();
            }
        }
    }
}
