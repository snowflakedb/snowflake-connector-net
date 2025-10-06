using System;

namespace Snowflake.Data.Configuration
{
    internal enum EasyLoggingLogLevel
    {
        Off,
        Error,
        Warn,
        Info,
        Debug,
        Trace
    }

    internal static class EasyLoggingLogLevelExtensions
    {
        public static EasyLoggingLogLevel From(string logLevel)
        {
            return (EasyLoggingLogLevel)Enum.Parse(typeof(EasyLoggingLogLevel), logLevel, true);
        }
    }
}
