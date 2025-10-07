using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Log
{
    internal class SFLoggerPair : SFLogger
    {
        private readonly SFLogger _snowflakeLogger;

        public SFLoggerPair(SFLogger snowflakeLogger)
        {
            _snowflakeLogger = snowflakeLogger;
        }

        public void Debug(string message, Exception ex = null)
        {
            if (!IsDebugEnabled())
                return;
            message = SecretDetector.MaskSecrets(message).maskedText;
            _snowflakeLogger.Debug(message, ex);
            SFLoggerFactory.s_customLogger.LogDebug(FormatBrackets(message), ex);
        }

        public void Info(string message, Exception ex = null)
        {
            if (!IsInfoEnabled())
                return;
            message = SecretDetector.MaskSecrets(message).maskedText;
            _snowflakeLogger.Info(message, ex);
            SFLoggerFactory.s_customLogger.LogInformation(FormatBrackets(message), ex);
        }

        public void Warn(string message, Exception ex = null)
        {
            if (!IsWarnEnabled())
                return;
            message = SecretDetector.MaskSecrets(message).maskedText;
            _snowflakeLogger.Warn(message, ex);
            SFLoggerFactory.s_customLogger.LogWarning(FormatBrackets(message), ex);
        }

        public void Error(string message, Exception ex = null)
        {
            if (!IsErrorEnabled())
                return;
            message = SecretDetector.MaskSecrets(message).maskedText;
            _snowflakeLogger.Error(message, ex);
            SFLoggerFactory.s_customLogger.LogError(FormatBrackets(message), ex);
        }

        public bool IsDebugEnabled()
        {
            return _snowflakeLogger.IsDebugEnabled() ||
                SFLoggerFactory.s_customLogger.IsEnabled(LogLevel.Debug);
        }

        public bool IsInfoEnabled()
        {
            return _snowflakeLogger.IsInfoEnabled() ||
                SFLoggerFactory.s_customLogger.IsEnabled(LogLevel.Information);
        }

        public bool IsWarnEnabled()
        {
            return _snowflakeLogger.IsWarnEnabled() ||
                SFLoggerFactory.s_customLogger.IsEnabled(LogLevel.Warning);
        }

        public bool IsErrorEnabled()
        {
            return _snowflakeLogger.IsErrorEnabled() ||
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
