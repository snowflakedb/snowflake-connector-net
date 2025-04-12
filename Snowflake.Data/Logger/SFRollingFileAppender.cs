using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using System;
using System.IO;
using System.Linq;

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

            if (!FileOperations.Instance.Exists(_logFilePath))
            {
                Console.Error.WriteLine("File does not exist: " + _logFilePath);
            }
            FileOperations.Instance.Write(_logFilePath, formattedMessage, null, true);
            if (ex != null)
                FileOperations.Instance.Write(_logFilePath, ex.Message, null, true);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("Encountered an error while writing log to file: " + e.Message);
            Console.Error.WriteLine("Log: " + message);
        }
    }

    public void ActivateOptions()
    {
        var logDir = Path.GetDirectoryName(_logFilePath);
        if (!DirectoryOperations.Instance.Exists(logDir))
        {
            DirectoryOperations.Instance.CreateDirectory(logDir);
        }
        if (!FileOperations.Instance.Exists(_logFilePath))
        {
            FileOperations.Instance.Create(_logFilePath).Dispose();
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

        if (!FileOperations.Instance.Exists(_logFilePath))
            FileOperations.Instance.Create(_logFilePath).Dispose();
    }
}
