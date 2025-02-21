/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

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
            return (EasyLoggingLogLevel) Enum.Parse(typeof(EasyLoggingLogLevel), logLevel, true);
        }
    }
}
