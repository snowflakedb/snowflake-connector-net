using System;
using System.IO;

namespace Snowflake.Data.Core.Tools
{
    public class DirectoryInformation
    {
        private readonly bool _exists;

        private readonly DateTime? _creationTimeUtc;

        public DirectoryInformation(DirectoryInfo directoryInfo)
        {
            _exists = directoryInfo.Exists;
            _creationTimeUtc = directoryInfo.CreationTimeUtc;
        }

        internal DirectoryInformation(bool exists, DateTime? creationTimeUtc)
        {
            _exists = exists;
            _creationTimeUtc = creationTimeUtc;
        }

        public bool IsCreatedEarlierThanSeconds(int seconds) => _exists && _creationTimeUtc?.AddSeconds(seconds) < DateTime.UtcNow;

        public bool Exists() => _exists;
    }
}
