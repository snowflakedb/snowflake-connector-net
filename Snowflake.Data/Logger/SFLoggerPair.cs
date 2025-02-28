/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Log
{
    internal class SFLoggerPair : SFLogger
    {
        private readonly SFLogger s_snowflakeLogger;

        public SFLoggerPair(SFLogger snowflakeLogger, ILogger customLogger)
        {
            s_snowflakeLogger = snowflakeLogger;
        }

        public void Debug(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Debug(message, ex);
            SFLoggerFactory.s_customLogger.LogDebug(FormatBrackets(message), ex);
        }

        public void Info(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Info(message, ex);
            SFLoggerFactory.s_customLogger.LogInformation(FormatBrackets(message), ex);
        }

        public void Warn(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Warn(message, ex);
            SFLoggerFactory.s_customLogger.LogWarning(FormatBrackets(message), ex);
        }

        public void Error(string message, Exception ex = null)
        {
            message = SecretDetector.MaskSecrets(message).maskedText;
            s_snowflakeLogger.Error(message, ex);
            SFLoggerFactory.s_customLogger.LogError(FormatBrackets(message), ex);
        }

        public bool IsDebugEnabled()
        {
            return s_snowflakeLogger.IsDebugEnabled() ||
                SFLoggerFactory.s_customLogger.IsEnabled(LogLevel.Debug);
        }

        public bool IsInfoEnabled()
        {
            return s_snowflakeLogger.IsInfoEnabled() ||
                SFLoggerFactory.s_customLogger.IsEnabled(LogLevel.Information);
        }

        public bool IsWarnEnabled()
        {
            return s_snowflakeLogger.IsWarnEnabled() ||
                SFLoggerFactory.s_customLogger.IsEnabled(LogLevel.Warning);
        }

        public bool IsErrorEnabled()
        {
            return s_snowflakeLogger.IsErrorEnabled() ||
                SFLoggerFactory.s_customLogger.IsEnabled(LogLevel.Error);
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
