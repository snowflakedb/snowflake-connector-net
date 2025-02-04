/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System;
using System.IO;
using System.Linq;
using System.Threading;

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

    public string Name => _name;

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
    internal long _maximumFileSizeInBytes;
    internal int _maxSizeRollBackups;
    internal PatternLayout _patternLayout;

    public SFRollingFileAppender() { }

    public string Name => _name;

    public void Append(string logLevel, string message, Type type, Exception ex = null)
    {
        var formattedMessage = _patternLayout.Format(logLevel, message, type);
        try
        {
            if (LogFileIsTooLarge())
            {
                RollLogFile();
            }

            using (FileStream fs = new FileStream(_logFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
            {
                using (var writer = new StreamWriter(fs))
                {
                    writer.Write(formattedMessage);
                    if (ex != null)
                    {
                        writer.WriteLine(ex.Message);
                    }
                }
            }
        }
        catch (Exception logEx)
        {
            Console.WriteLine($"Unable to log the following message:\n{formattedMessage}" +
                $"Due to the error: {logEx.Message}\n");
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
            File.Create(_logFilePath).Dispose();
        }
    }

    private bool LogFileIsTooLarge()
    {
        FileInfo fileInfo = new FileInfo(_logFilePath);
        return fileInfo.Exists && fileInfo.Length > _maximumFileSizeInBytes;
    }

    private void RollLogFile()
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
