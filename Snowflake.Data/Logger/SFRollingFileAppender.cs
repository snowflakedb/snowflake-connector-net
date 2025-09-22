using Mono.Unix;
using Mono.Unix.Native;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

internal class SFRollingFileAppender : SFAppender
{
    internal string LogFilePath { get; set; }

    internal long MaximumFileSizeInBytes { get; set; }

    internal int MaxSizeRollBackups { get; set; }

    internal PatternLayout PatternLayout { get; set; }

    private readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    public void Append(string logLevel, string message, Type type, Exception ex = null)
    {
        var formattedMessage = PatternLayout.Format(logLevel, message, type);
        try
        {
            if (LogFileIsTooLarge())
            {
                RollLogFile();
            }

            if (IsWindows)
            {
                FileOperations.Instance.Append(LogFilePath, formattedMessage, ex);
            }
            else
            {
                UnixOperations.Instance.AppendToFile(LogFilePath, formattedMessage, SFCredentialManagerFileImpl.Instance.ValidateLogFilePermissions, ex);
            }
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
            FileOperations.Instance.Create(LogFilePath, EasyLoggingStarter.s_logFileUnixPermissions).Dispose();
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
            FileOperations.Instance.Create(LogFilePath, EasyLoggingStarter.s_logFileUnixPermissions).Dispose();
    }
}
