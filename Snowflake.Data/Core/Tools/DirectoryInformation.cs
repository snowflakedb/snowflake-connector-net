using System;
using System.IO;

namespace Snowflake.Data.Core.Tools
{
    internal class DirectoryInformation
    {
        public bool Exists { get; private set; }

        public DateTime? CreationTimeUtc { get; private set; }

        public string FullName { get; private set; }

        public DirectoryInformation(DirectoryInfo directoryInfo)
        {
            Exists = directoryInfo.Exists;
            CreationTimeUtc = directoryInfo.CreationTimeUtc;
            FullName = directoryInfo.FullName;
        }

        internal DirectoryInformation(bool exists, DateTime? creationTimeUtc)
        {
            Exists = exists;
            CreationTimeUtc = creationTimeUtc;
        }

        public bool IsCreatedEarlierThanSeconds(int seconds, DateTime utcNow) =>
            Exists && CreationTimeUtc?.AddSeconds(seconds) < utcNow;
    }
}
