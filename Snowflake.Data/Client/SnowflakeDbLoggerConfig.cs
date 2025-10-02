using Microsoft.Extensions.Logging;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbLoggerConfig
    {
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
