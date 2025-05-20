using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Log
{
    internal class SFLoggerFactory
    {
        internal static bool s_isSFLoggerEnabled = false;

        internal static ILogger s_customLogger = new LoggerEmptyImpl();

        internal static SFLogger GetLogger<T>()
        {
            return new SFLoggerPair(GetSFLogger<T>());
        }

        internal static SFLogger GetSFLogger<T>()
        {
            var logger = new SFLoggerImpl(typeof(T));
            if (!s_isSFLoggerEnabled)
            {
                SFLoggerImpl.SetLevel(LoggingEvent.OFF); // Logger is disabled by default and can be enabled by the EasyLogging feature
            }
            return logger;
        }
    }
}
