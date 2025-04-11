using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

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

            FileOperations.Instance.Write(_logFilePath, formattedMessage, null, true);
            if (ex != null)
                FileOperations.Instance.Write(_logFilePath, ex.Message, null, true);
        }
        catch
        {
            Console.Error.WriteLine("Encountered an error while writing log to file");
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
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                FileOperations.Instance.Create(_logFilePath).Dispose();
            else
                FileOperations.Instance.Create(_logFilePath);
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

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            FileOperations.Instance.Create(_logFilePath).Dispose();
        else
            FileOperations.Instance.Create(_logFilePath);
    }
}
