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
            isLoggerEnabled = false;
        }

        public static void enableLogger()
        {
            isLoggerEnabled = true;
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
            // If true, return the default/specified logger
            if (isLoggerEnabled)
            {
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
