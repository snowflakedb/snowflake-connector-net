/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

public static class SFLogRepository
{
    internal static SFLogger s_rootLogger = s_rootLogger = new SFLoggerImpl(typeof(SFLogRepository));

    internal static SFLogger GetRootLogger()
    {
        return s_rootLogger;
    }
}

internal class PatternLayout
{
    internal string _conversionPattern;

    public PatternLayout() { }

    public string Format(string logLevel, string message, Type type)
    {
        var formattedMessage = _conversionPattern
            .Replace("%date", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"))
            .Replace("%t", Thread.CurrentThread.ManagedThreadId.ToString())
            .Replace("%-5level", logLevel)
            .Replace("%logger", type.ToString())
            .Replace("%message", message)
            .Replace("%newline", "\n");

        return formattedMessage;
    }
}

internal class SFConsoleAppender : SFAppender
{
    internal string _name;
    internal PatternLayout _patternLayout;

    public SFConsoleAppender() { }

    public void Append(string logLevel, string message, Type type, Exception ex = null)
    {
        var formattedMessage = _patternLayout.Format(logLevel, message, type);
        Console.Write(formattedMessage);
        if (ex != null)
        {
            Console.WriteLine(ex.Message);
        }
    }
}

internal class SFRollingFileAppender : SFAppender
{
    internal string _name;
    internal string _logFilePath;
    internal long _maximumFileSize;
    internal int _maxSizeRollBackups;
    internal PatternLayout _patternLayout;

    public SFRollingFileAppender() { }

    public void Append(string logLevel, string message, Type type, Exception ex = null)
    {
        try
        {
            if (FileIsTooLarge())
            {
                RollFile();
            }

            var formattedMessage = _patternLayout.Format(logLevel, message, type);
            using (var writer = new StreamWriter(_logFilePath, true))
            {
                writer.Write(formattedMessage);
                if (ex != null)
                {
                    writer.WriteLine(ex.Message);
                }
            }
        }
        catch (Exception logEx)
        {
            Console.WriteLine($"Failed to log message: {logEx.Message}");
        }
    }

    public void ActivateOptions()
    {
        var logDir = Path.GetDirectoryName(_logFilePath);
        if (!Directory.Exists(logDir))
        {
            Directory.CreateDirectory(logDir);
        }
        if (!File.Exists(_logFilePath))
        {
            var file = File.Create(_logFilePath);
            file.Close();
        }
    }

    private bool FileIsTooLarge()
    {
        FileInfo fileInfo = new FileInfo(_logFilePath);
        return fileInfo.Exists && fileInfo.Length > _maximumFileSize;
    }

    private void RollFile()
    {
        string rollFilePath = $"{_logFilePath}.{DateTime.Now:yyyyMMddHHmmss}.bak";
        File.Move(_logFilePath, rollFilePath);

        var logDirectory = Path.GetDirectoryName(_logFilePath);
        var logFileName = Path.GetFileName(_logFilePath);
        var rollFiles = Directory.GetFiles(logDirectory, $"{logFileName}.*.bak")
            .OrderByDescending(f => f)
            .Skip(_maxSizeRollBackups);
        foreach (var oldRollFile in rollFiles)
        {
            File.Delete(oldRollFile);
        }
    }
}

public class SFLoggerImpl : SFLogger
{
    private readonly Type _type;
    internal readonly List<SFAppender> _appenders;
    internal LoggingEvent _level;

    private bool _isDebugEnabled;
    private bool _isInfoEnabled;
    private bool _isWarnEnabled;
    private bool _isErrorEnabled;
    private bool _isFatalEnabled;

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
        _isFatalEnabled = enabled;

        if (enabled)
        {
            switch (_level)
            {
                case LoggingEvent.TRACE:
                case LoggingEvent.DEBUG:
                    break;
                case LoggingEvent.FATAL:
                    _isErrorEnabled = false;
                    goto case LoggingEvent.ERROR;
                case LoggingEvent.ERROR:
                    _isWarnEnabled = false;
                    goto case LoggingEvent.WARN;
                case LoggingEvent.WARN:
                    _isInfoEnabled = false;
                    goto case LoggingEvent.INFO;
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

    public bool IsFatalEnabled()
    {
        return SFLogRepository.s_rootLogger == this ?
            _isFatalEnabled :
            SFLogRepository.s_rootLogger.IsFatalEnabled();
    }

    public void Debug(string msg, Exception ex = null)
    {
        if (IsDebugEnabled())
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            Log(LoggingEvent.DEBUG.ToString(), msg, ex);
        }
    }

    public void Info(string msg, Exception ex = null)
    {
        if (IsInfoEnabled())
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            Log(LoggingEvent.INFO.ToString(), msg, ex);
        }
    }

    public void Warn(string msg, Exception ex = null)
    {
        if (IsWarnEnabled())
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            Log(LoggingEvent.WARN.ToString(), msg, ex);
        }
    }


    public void Error(string msg, Exception ex = null)
    {
        if (IsErrorEnabled())
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            Log(LoggingEvent.ERROR.ToString(), msg, ex);
        }
    }

    public void Fatal(string msg, Exception ex = null)
    {
        if (IsFatalEnabled())
        {
            msg = SecretDetector.MaskSecrets(msg).maskedText;
            Log(LoggingEvent.FATAL.ToString(), msg, ex);
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
