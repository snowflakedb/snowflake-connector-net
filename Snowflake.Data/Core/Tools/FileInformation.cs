using System;
using System.IO;

namespace Snowflake.Data.Core.Tools
{
    internal class FileInformation
    {
        public bool Exists { get; internal set; }

        public DateTime LastWriteTimeUtc { get; internal set; }

        public FileInformation(FileInfo fileInfo)
        {
            Exists = fileInfo.Exists;
            LastWriteTimeUtc = fileInfo.LastWriteTimeUtc;
        }

        public FileInformation()
        {
        }
    }
}
