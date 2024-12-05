/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Extensions.Logging;
using Snowflake.Data.Log;
using System;

public class SFLoggerPair
{
    private static SFLogger s_snowflakeLogger;
    private static ILogger s_customLogger;

    SFLoggerPair(SFLogger snowflakLogger, ILogger customLogger)
    {
        s_snowflakeLogger = snowflakLogger;
        s_customLogger = customLogger;
    }

    internal static SFLoggerPair GetLoggerPair<T>()
    {
        return new SFLoggerPair(SFLoggerFactory.GetSFLogger<T>(), SFLoggerFactory.GetCustomLogger<T>());
    }

    internal void LogDebug(string message, Exception ex = null)
    {
        s_snowflakeLogger.Debug(message, ex);
        s_customLogger.LogDebug(message, ex);
    }

    internal void LogInformation(string message, Exception ex = null)
    {
        s_snowflakeLogger.Information(message, ex);
        s_customLogger.LogInformation(message, ex);
    }

    internal void LogWarning(string message, Exception ex = null)
    {
        s_snowflakeLogger.Warning(message, ex);
        s_customLogger.LogWarning(message, ex);
    }

    internal void LogError(string message, Exception ex = null)
    {
        s_snowflakeLogger.Error(message, ex);
        s_customLogger.LogError(message, ex);
    }
}
