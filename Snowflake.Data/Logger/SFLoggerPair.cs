using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Log;

internal class SFLoggerPair : SFLogger
{
    private readonly SFLogger _snowflakeLogger;

    public SFLoggerPair(SFLogger snowflakeLogger)
    {
        _snowflakeLogger = snowflakeLogger;
    }

    public void Trace(string message, Exception ex = null)
    {
        if (!IsTraceEnabled())
            return;
        message = SecretDetector.Instance.MaskSecrets(message).MaskedText;
        _snowflakeLogger.Trace(message, new MaskedException(ex));
        SFLoggerFactory.CustomLogger.LogTrace(FormatBrackets(message), new MaskedException(ex));
    }

    public void Debug(string message, Exception ex = null)
    {
        if (!IsDebugEnabled())
            return;
        message = SecretDetector.Instance.MaskSecrets(message).MaskedText;
        _snowflakeLogger.Debug(message, new MaskedException(ex));
        SFLoggerFactory.CustomLogger.LogDebug(FormatBrackets(message), new MaskedException(ex));
    }

    public void Info(string message, Exception ex = null)
    {
        if (!IsInfoEnabled())
            return;
        message = SecretDetector.Instance.MaskSecrets(message).MaskedText;
        _snowflakeLogger.Info(message, new MaskedException(ex));
        SFLoggerFactory.CustomLogger.LogInformation(FormatBrackets(message), new MaskedException(ex));
    }

    public void Warn(string message, Exception ex = null)
    {
        if (!IsWarnEnabled())
            return;
        message = SecretDetector.Instance.MaskSecrets(message).MaskedText;
        _snowflakeLogger.Warn(message, new MaskedException(ex));
        SFLoggerFactory.CustomLogger.LogWarning(FormatBrackets(message), new MaskedException(ex));
    }

    public void Error(string message, Exception ex = null)
    {
        if (!IsErrorEnabled())
            return;
        message = SecretDetector.Instance.MaskSecrets(message).MaskedText;
        _snowflakeLogger.Error(message, new MaskedException(ex));
        SFLoggerFactory.CustomLogger.LogError(FormatBrackets(message), new MaskedException(ex));
    }

    public bool IsTraceEnabled()
    {
        return _snowflakeLogger.IsTraceEnabled() ||
               SFLoggerFactory.CustomLogger.IsEnabled(LogLevel.Trace);
    }

    public bool IsDebugEnabled()
    {
        return _snowflakeLogger.IsDebugEnabled() ||
               SFLoggerFactory.CustomLogger.IsEnabled(LogLevel.Debug);
    }

    public bool IsInfoEnabled()
    {
        return _snowflakeLogger.IsInfoEnabled() ||
               SFLoggerFactory.CustomLogger.IsEnabled(LogLevel.Information);
    }

    public bool IsWarnEnabled()
    {
        return _snowflakeLogger.IsWarnEnabled() ||
               SFLoggerFactory.CustomLogger.IsEnabled(LogLevel.Warning);
    }

    public bool IsErrorEnabled()
    {
        return _snowflakeLogger.IsErrorEnabled() ||
               SFLoggerFactory.CustomLogger.IsEnabled(LogLevel.Error);
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

    private class MaskedException : Exception
    {
        private readonly Exception _innerException;

        public MaskedException(Exception innerException)
        {
            _innerException = innerException;
        }

        public override string ToString() => SecretDetector.Instance.MaskSecrets(_innerException?.ToString()).MaskedText;

        public override string Message => SecretDetector.Instance.MaskSecrets(_innerException?.Message).MaskedText;

        public override string StackTrace => SecretDetector.Instance.MaskSecrets(_innerException?.StackTrace).MaskedText;
    }
}
