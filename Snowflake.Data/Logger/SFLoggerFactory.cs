/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using log4net;
using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Log
{
    public class SFLoggerFactory
    {
        private static bool isLoggerEnabled = false;

        private static bool isSimpleLoggerEnabled = true;

        private static SFLogger simpleLogger = null;

        private static ILogger customLogger = null;

        private SFLoggerFactory()
        {
        }

        public static void DisableSimpleLogger()
        {
            isSimpleLoggerEnabled = false;
        }

        public static void EnableSimpleLogger()
        {
            isSimpleLoggerEnabled = true;
        }

        public static void DisableLogger()
        {
            isLoggerEnabled = false;
        }

        public static void EnableLogger()
        {
            isLoggerEnabled = true;
        }

        public static void UseDefaultLogger()
        {
            customLogger = null;
        }

        public static void SetCustomLogger(ILogger customLogger)
        {
            SFLoggerFactory.customLogger = customLogger;
        }

        internal static SFLogger GetSimpleLogger<T>()
        {
            // If true, return the default/specified logger
            if (isSimpleLoggerEnabled)
            {
                ILog loggerL = LogManager.GetLogger(typeof(T));
                return new Log4NetImpl(loggerL);
            }
            // Else, return the empty logger implementation which outputs nothing
            else
            {
                return new SFLoggerEmptySimpleImpl();
            }
        }

        internal static ILogger GetLogger<T>()
        {
            // If true, return the default/specified logger
            if (isLoggerEnabled)
            {
                // If no logger specified, use the default logger: Microsoft's console logger
                if (customLogger == null)
                {
                    ILoggerFactory factory = LoggerFactory.Create(
                        builder => builder
                        .AddConsole()
                        .SetMinimumLevel(LogLevel.Trace)
                    );

                    return factory.CreateLogger<T>();
                }
                return customLogger;
            }
            // Else, return the empty logger implementation which outputs nothing
            else
            {
                return new SFLoggerEmptyImpl();
            }
        }
    }
}
