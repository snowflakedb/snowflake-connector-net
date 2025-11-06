using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Log
{
    internal class SFLoggerFactory
    {
        internal static ILogger s_customLogger = new LoggerEmptyImpl();

        internal static SFLogger GetLogger<T>()
        {
            return new SFLoggerPair(GetSFLogger<T>());
        }

        internal static SFLogger GetSFLogger<T>()
        {
            var logger = new SFLoggerImpl(typeof(T));
            return logger;
        }
    }
}
