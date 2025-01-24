/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Log
{
    public class SFLoggerPair : SFLogger
    {
        private static SFLogger s_snowflakeLogger;
        internal static ILogger s_customLogger;

        public SFLoggerPair(SFLogger snowflakeLogger, ILogger customLogger)
        {
            s_snowflakeLogger = snowflakeLogger;
            s_customLogger = customLogger;
        }

        public void Debug(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Debug(message, ex);
            s_customLogger.LogDebug(FormatBrackets(message), ex);
        }

        public void Info(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Info(message, ex);
            s_customLogger.LogInformation(FormatBrackets(message), ex);
        }

        public void Warn(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Warn(message, ex);
            s_customLogger.LogWarning(FormatBrackets(message), ex);
        }

        public void Error(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Error(message, ex);
            s_customLogger.LogError(FormatBrackets(message), ex);
        }

        public void Fatal(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Fatal(message, ex);
            s_customLogger.LogCritical(FormatBrackets(message), ex);
        }

        public bool IsDebugEnabled()
        {
            return s_snowflakeLogger.IsDebugEnabled() ||
                s_customLogger.IsEnabled(LogLevel.Debug);
        }

        public bool IsInfoEnabled()
        {
            return s_snowflakeLogger.IsInfoEnabled() ||
                s_customLogger.IsEnabled(LogLevel.Information);
        }

        public bool IsWarnEnabled()
        {
            return s_snowflakeLogger.IsWarnEnabled() ||
                s_customLogger.IsEnabled(LogLevel.Warning);
        }

        public bool IsErrorEnabled()
        {
            return s_snowflakeLogger.IsErrorEnabled() ||
                s_customLogger.IsEnabled(LogLevel.Error);
        }

        public bool IsFatalEnabled()
        {
            return s_snowflakeLogger.IsFatalEnabled() ||
                s_customLogger.IsEnabled(LogLevel.Critical);
        }

        public List<SFAppender> GetAppenders()
        {
            throw new NotImplementedException();
        }

        public void AddAppender(SFAppender appender)
        {
            throw new NotImplementedException();
        }

        public void RemoveAppender(SFAppender appender)
        {
            throw new NotImplementedException();
        }

        public void SetLevel(LoggingEvent level)
        {
            throw new NotImplementedException();
        }

        private string FormatBrackets(string message)
        {
            var sb = new StringBuilder(message).Replace("{", "{{").Replace("}", "}}");
            return sb.ToString();
        }
    }
}
