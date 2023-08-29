using System;

namespace Snowflake.Data.Configuration
{
    internal enum EasyLoggingLogLevel
    {
        OFF,
        ERROR,
        WARN,
        INFO,
        DEBUG,
        TRACE
    }

    internal static class EasyLoggingLogLevelExtensions
    {
        public static EasyLoggingLogLevel From(string logLevel)
        {
            return (EasyLoggingLogLevel) Enum.Parse(typeof(EasyLoggingLogLevel), logLevel, true);
        }
    }
}
