/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System;
using System.Collections.Generic;

internal class SFLoggerImpl : SFLogger
{
    private readonly Type _type;
    internal readonly List<SFAppender> _appenders;
    internal LoggingEvent _level;

    private bool _isDebugEnabled;
    private bool _isInfoEnabled;
    private bool _isWarnEnabled;
    private bool _isErrorEnabled;

    internal SFLoggerImpl(Type type, LoggingEvent level = LoggingEvent.DEBUG)
    {
        _appenders = new List<SFAppender>();
        _type = type;
        SetLevel(level);
    }

    public void SetLevel(LoggingEvent level)
    {
        _level = level;
        SetEnableValues();
    }

    private void SetEnableValues()
    {
        var enabled = _level != LoggingEvent.OFF;
        _isDebugEnabled = enabled;
        _isInfoEnabled = enabled;
        _isWarnEnabled = enabled;
        _isErrorEnabled = enabled;

        if (enabled)
        {
            switch (_level)
            {
                case LoggingEvent.TRACE:
                case LoggingEvent.DEBUG:
                    break;
                case LoggingEvent.ERROR:
                    _isWarnEnabled = false;
                    _isInfoEnabled = false;
                    _isDebugEnabled = false;
                    break;
                case LoggingEvent.WARN:
                    _isInfoEnabled = false;
                    _isDebugEnabled = false;
                    break;
                case LoggingEvent.INFO:
                    _isDebugEnabled = false;
                    break;
            }
        }
    }

    public List<SFAppender> GetAppenders()
    {
        return _appenders;
    }

    public void AddAppender(SFAppender appender)
    {
        _appenders.Add(appender);
    }

    public void RemoveAppender(SFAppender appender)
    {
        _appenders.Remove(appender);
    }

    public bool IsDebugEnabled()
    {
        return SFLogRepository.s_rootLogger == this ?
            _isDebugEnabled :
            SFLogRepository.s_rootLogger.IsDebugEnabled();
    }

    public bool IsInfoEnabled()
    {
        return SFLogRepository.s_rootLogger == this ?
            _isInfoEnabled :
            SFLogRepository.s_rootLogger.IsInfoEnabled();
    }

    public bool IsWarnEnabled()
    {
        return SFLogRepository.s_rootLogger == this ?
            _isWarnEnabled :
            SFLogRepository.s_rootLogger.IsWarnEnabled();
    }

    public bool IsErrorEnabled()
    {
        return SFLogRepository.s_rootLogger == this ?
            _isErrorEnabled :
            SFLogRepository.s_rootLogger.IsErrorEnabled();
    }

    public void Debug(string msg, Exception ex = null)
    {
        if (IsDebugEnabled())
        {
            Log(LoggingEvent.DEBUG.ToString(), msg, ex);
        }
    }

    public void Info(string msg, Exception ex = null)
    {
        if (IsInfoEnabled())
        {
            Log(LoggingEvent.INFO.ToString(), msg, ex);
        }
    }

    public void Warn(string msg, Exception ex = null)
    {
        if (IsWarnEnabled())
        {
            Log(LoggingEvent.WARN.ToString(), msg, ex);
        }
    }


    public void Error(string msg, Exception ex = null)
    {
        if (IsErrorEnabled())
        {
            Log(LoggingEvent.ERROR.ToString(), msg, ex);
        }
    }

    private void Log(string logLevel, string logMessage, Exception ex = null)
    {
        var rootAppenders = SFLogRepository.s_rootLogger.GetAppenders();
        var appenders = rootAppenders.Count > 0 ? rootAppenders : _appenders;
        foreach (var appender in appenders)
        {
            appender.Append(logLevel, logMessage, _type, ex);
        }
    }
}
