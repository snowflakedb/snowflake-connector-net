/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using log4net;

namespace Snowflake.Data.Log
{
    class SFLoggerFactory
    {
        private static bool isLoggerEnabled = true;

        private static SFLogger logger = null;

        private SFLoggerFactory()
        {
        }

        public static void disableLogger()
        {
            System.Console.WriteLine($"disableLogger before: {isLoggerEnabled}");
            isLoggerEnabled = false;
            System.Console.WriteLine($"disableLogger after: {isLoggerEnabled}");
        }

        public static void enableLogger()
        {
            System.Console.WriteLine($"enableLogger before: {isLoggerEnabled}");
            isLoggerEnabled = true;
            System.Console.WriteLine($"enableLogger after: {isLoggerEnabled}");
        }

        public static void useDefaultLogger()
        {
            logger = null;
        }

        public static void Instance(SFLogger customLogger)
        {            
            logger = customLogger;
        }

        public static SFLogger GetLogger<T>()
        {
            System.Console.WriteLine($"GetLogger isLoggerEnabled: {isLoggerEnabled}");

            // If true, return the default/specified logger
            if (isLoggerEnabled)
            {
                System.Console.WriteLine($"GetLogger logger: {logger}");

                // If no logger specified, use the default logger: log4net
                if (logger == null)
                {
                    ILog loggerL = LogManager.GetLogger(typeof(T));
                    return new Log4NetImpl(loggerL);
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
