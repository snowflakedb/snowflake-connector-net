using System;
using log4net.Core;
using Snowflake.Data.Configuration;

namespace Snowflake.Data.Log
{
    internal class EasyLoggingLevelMapper
    {
        public static readonly EasyLoggingLevelMapper Instance = new EasyLoggingLevelMapper();

        public Level ToLog4NetLevel(EasyLoggingLogLevel level)
        {
            switch (level)
            {
                case EasyLoggingLogLevel.Off: return Level.Off;
                case EasyLoggingLogLevel.Error: return Level.Error;
                case EasyLoggingLogLevel.Warn: return Level.Warn;
                case EasyLoggingLogLevel.Info: return Level.Info;
                case EasyLoggingLogLevel.Debug: return Level.Debug;
                case EasyLoggingLogLevel.Trace: return Level.Trace;
                default: throw new Exception("Unknown log level");
            }
        }
    }
}
