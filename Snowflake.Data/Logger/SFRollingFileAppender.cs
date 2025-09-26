using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Logger;
using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace Snowflake.Data.Log
{
    internal class SFRollingFileAppender : SFAppender
    {
        internal string LogFilePath { get; set; }

        internal long MaximumFileSizeInBytes { get; set; }

        internal int MaxSizeRollBackups { get; set; }

        internal PatternLayout PatternLayout { get; set; }

        private readonly FileOperations _fileOperations;

        private readonly UnixOperations _unixOperations;

        private readonly DirectoryOperations _directoryOperations;

        public SFRollingFileAppender(FileOperations fileOperations, UnixOperations unixOperations, DirectoryOperations directoryOperations)
        {
            _fileOperations = fileOperations;
            _unixOperations = unixOperations;
            _directoryOperations = directoryOperations;
        }

        private readonly bool _isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        public void Append(string logLevel, string message, Type type, Exception ex = null)
        {
            var formattedMessage = PatternLayout.Format(logLevel, message, type);
            try
            {
                long fileSize;
                if (_isWindows)
                {
                    _fileOperations.Append(LogFilePath, formattedMessage, ex?.ToString());
                    fileSize = LogFileSize();
                }
                else
                {
                    fileSize = _unixOperations.AppendToFile(LogFilePath, formattedMessage, ex?.ToString(),
                        EasyLoggerValidator.Instance.ValidateLogFilePermissions, EasyLoggingStarter.Instance._logFileUnixPermissions);
                }
                if (fileSize > MaximumFileSizeInBytes)
                {
                    RollLogFile();
                }
            }
            catch
            {
                try
                {
                    Console.Error.WriteLine("Encountered an error while writing log to file");
                }
                catch (Exception)
                {
                }
            }
        }

        public void ActivateOptions()
        {
            var logDir = Path.GetDirectoryName(LogFilePath);
            if (!_directoryOperations.Exists(logDir))
                _directoryOperations.CreateDirectory(logDir);
            if (!_fileOperations.Exists(LogFilePath))
                _fileOperations.Create(LogFilePath, EasyLoggingStarter.Instance._logFileUnixPermissions).Dispose();
        }

        private long LogFileSize()
        {
            var fileInfo = _fileOperations.GetFileInfo(LogFilePath);
            return fileInfo.Exists ? fileInfo.Length : 0;
        }

        private void RollLogFile()
        {
            string rollFilePath = $"{LogFilePath}.{DateTime.Now:yyyyMMddHHmmss}.bak";
            File.Move(LogFilePath, rollFilePath);

            var logDirectory = Path.GetDirectoryName(LogFilePath);
            var logFileName = Path.GetFileName(LogFilePath);
            var rollFiles = _directoryOperations.GetFiles(logDirectory, $"{logFileName}.*.bak")
                .OrderByDescending(f => f)
                .Skip(MaxSizeRollBackups);
            foreach (var oldRollFile in rollFiles)
            {
                File.Delete(oldRollFile);
            }

            if (!_fileOperations.Exists(LogFilePath))
                _fileOperations.Create(LogFilePath, EasyLoggingStarter.Instance._logFileUnixPermissions).Dispose();
        }
    }
}
