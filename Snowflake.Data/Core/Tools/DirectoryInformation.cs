using System;
using System.IO;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    public class DirectoryInformation
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<DirectoryInformation>();

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

        public bool IsCreatedEarlierThanSeconds(int seconds, DateTime utcNow)
        {
            s_logger.Warn($"Now is {utcNow}");
            s_logger.Warn($"CreationTimeUtc is {_creationTimeUtc}");
            s_logger.Warn($"CreationTimeUtc + {60} seconds is {_creationTimeUtc?.AddSeconds(seconds)}");
            s_logger.Warn($"Result of date comparison is {_creationTimeUtc?.AddSeconds(seconds) < utcNow}");
            return _exists && _creationTimeUtc?.AddSeconds(seconds) < utcNow;
        }

        public bool Exists() => _exists;
    }
}
