/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Log
{
    public class SFLoggerFactory
    {
        private static bool isLoggerEnabled = false;

        private static ILogger logger = null;

        private SFLoggerFactory()
        {
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
            logger = null;
        }

        public static void SetCustomLogger(ILogger customLogger)
        {            
            logger = customLogger;
        }

        internal static ILogger GetLogger<T>()
        {
            // If true, return the default/specified logger
            if (isLoggerEnabled)
            {
                // If no logger specified, use the default logger: Microsoft's console logger
                if (logger == null)
                {
                    ILoggerFactory factory = LoggerFactory.Create(
                        builder => builder
                        .AddConsole()
                        .SetMinimumLevel(LogLevel.Trace)
                    );

                    return factory.CreateLogger<T>();
                }
                return logger;
            }
            // Else, return the empty logger implementation which outputs nothing
            else
            {
                return new SFLoggerEmptyImpl();
            }
        }
    }
}
