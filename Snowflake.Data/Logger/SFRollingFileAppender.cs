using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using System;
using System.IO;
using System.Linq;

internal class SFRollingFileAppender : SFAppender
{
    internal string LogFilePath { get; set; }

    internal long MaximumFileSizeInBytes { get; set; }

    internal int MaxSizeRollBackups { get; set; }

    internal PatternLayout PatternLayout { get; set; }

    public void Append(string logLevel, string message, Type type, Exception ex = null)
    {
        var formattedMessage = PatternLayout.Format(logLevel, message, type);
        try
        {
            if (LogFileIsTooLarge())
            {
                RollLogFile();
            }

            FileOperations.Instance.Write(LogFilePath, formattedMessage, null, true);
            if (ex != null)
                FileOperations.Instance.Write(LogFilePath, ex.Message, null, true);
        }
        catch
        {
            Console.Error.WriteLine("Encountered an error while writing log to file");
        }
    }

    public void ActivateOptions()
    {
        var logDir = Path.GetDirectoryName(LogFilePath);
        if (!DirectoryOperations.Instance.Exists(logDir))
            DirectoryOperations.Instance.CreateDirectory(logDir);
        if (!FileOperations.Instance.Exists(LogFilePath))
            FileOperations.Instance.Create(LogFilePath).Dispose();
    }

    private bool LogFileIsTooLarge()
    {
        FileInfo fileInfo = new FileInfo(LogFilePath);
        return fileInfo.Exists && fileInfo.Length > MaximumFileSizeInBytes;
    }

    private void RollLogFile()
    {
        string rollFilePath = $"{LogFilePath}.{DateTime.Now:yyyyMMddHHmmss}.bak";
        File.Move(LogFilePath, rollFilePath);

        var logDirectory = Path.GetDirectoryName(LogFilePath);
        var logFileName = Path.GetFileName(LogFilePath);
        var rollFiles = Directory.GetFiles(logDirectory, $"{logFileName}.*.bak")
            .OrderByDescending(f => f)
            .Skip(MaxSizeRollBackups);
        foreach (var oldRollFile in rollFiles)
        {
            File.Delete(oldRollFile);
        }

        if (!FileOperations.Instance.Exists(LogFilePath))
            FileOperations.Instance.Create(LogFilePath).Dispose();
    }
}
