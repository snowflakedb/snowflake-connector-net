using System;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Log
{
    internal class EasyLoggingLevelMapper
    {
        public static readonly EasyLoggingLevelMapper Instance = new EasyLoggingLevelMapper();

        public LoggingEvent ToLoggingEventLevel(EasyLoggingLogLevel level)
        {
            switch (level)
            {
                case EasyLoggingLogLevel.Off: return LoggingEvent.OFF;
                case EasyLoggingLogLevel.Error: return LoggingEvent.ERROR;
                case EasyLoggingLogLevel.Warn: return LoggingEvent.WARN;
                case EasyLoggingLogLevel.Info: return LoggingEvent.INFO;
                case EasyLoggingLogLevel.Debug: return LoggingEvent.DEBUG;
                case EasyLoggingLogLevel.Trace: return LoggingEvent.TRACE;
                default: throw new Exception("Unknown log level");
            }
        }
    }
}
