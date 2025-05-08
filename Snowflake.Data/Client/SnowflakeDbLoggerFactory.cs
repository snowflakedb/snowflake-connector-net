using Microsoft.Extensions.Logging;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbLoggerFactory
    {
        public static void DisableCustomLogger()
        {
            SFLoggerFactory.s_isCustomLoggerEnabled = false;
        }

        public static void EnableCustomLogger()
        {
            SFLoggerFactory.s_isCustomLoggerEnabled = true;
        }

        public static void ResetCustomLogger()
        {
            SFLoggerFactory.s_customLogger = new LoggerEmptyImpl();
        }

        public static void SetCustomLogger(ILogger customLogger)
        {
            SFLoggerFactory.s_customLogger = customLogger;
        }
    }
}
