/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Extensions.Logging;
using System;
using System.Text;

namespace Snowflake.Data.Log
{
    public class SFLoggerPair
    {
        private static SFLogger s_snowflakeLogger;
        private static ILogger s_customLogger;

        SFLoggerPair(SFLogger snowflakeLogger, ILogger customLogger)
        {
            s_snowflakeLogger = snowflakeLogger;
            s_customLogger = customLogger;
        }

        internal static SFLoggerPair GetLoggerPair<T>()
        {
            return new SFLoggerPair(SFLoggerFactory.GetSFLogger<T>(), SFLoggerFactory.GetCustomLogger<T>());
        }

        internal void LogDebug(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Debug(message, ex);
            s_customLogger.LogDebug(FormatBrackets(message), ex);
        }

        internal void LogInformation(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Information(message, ex);
            s_customLogger.LogInformation(message, ex);
        }

        internal void LogWarning(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Warning(message, ex);
            s_customLogger.LogWarning(message, ex);
        }

        internal void LogError(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Error(message, ex);
            s_customLogger.LogError(message, ex);
        }

        internal bool IsDebugEnabled()
        {
            return s_snowflakeLogger.IsDebugEnabled() ||
                s_customLogger.IsEnabled(LogLevel.Debug);
        }

        private string FormatBrackets(string message)
        {
            var sb = new StringBuilder(message).Replace("{", "{{").Replace("}", "}}");
            return sb.ToString();
        }
    }
}
