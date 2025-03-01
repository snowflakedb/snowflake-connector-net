using System;
using System.IO;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{
    internal class SFCredentialManagerFileStorage
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerFileStorage>();

        internal const string CredentialCacheDirectoryEnvironmentName = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";

        internal const string CommonCacheDirectoryEnvironmentName = "XDG_CACHE_HOME";

        internal const string CommonCacheDirectoryName = ".cache";

        internal const string CredentialCacheDirName = "snowflake";

        internal const string CredentialCacheFileName = "credential_cache_v1.json";

        internal const string CredentialCacheLockName = CredentialCacheFileName + ".lck";

        public string JsonCacheDirectory { get; private set; }

        public string JsonCacheFilePath { get; private set; }

        public string JsonCacheLockPath { get; private set; }

        public SFCredentialManagerFileStorage(EnvironmentOperations environmentOperations)
        {
            var snowflakeEnvBasedDirectory = environmentOperations.GetEnvironmentVariable(CredentialCacheDirectoryEnvironmentName);
            if (!string.IsNullOrEmpty(snowflakeEnvBasedDirectory))
            {
                InitializeForDirectory(snowflakeEnvBasedDirectory);
                return;
            }
            var commonCacheEnvBasedDirectory = environmentOperations.GetEnvironmentVariable(CommonCacheDirectoryEnvironmentName);
            if (!string.IsNullOrEmpty(commonCacheEnvBasedDirectory))
            {
                InitializeForDirectory(Path.Combine(commonCacheEnvBasedDirectory, CredentialCacheDirName));
                return;
            }
            var homeBasedDirectory = HomeDirectoryProvider.HomeDirectory(environmentOperations);
            if (string.IsNullOrEmpty(homeBasedDirectory))
            {
                throw new Exception("Unable to identify credential cache directory");
            }
            InitializeForDirectory(Path.Combine(homeBasedDirectory, CommonCacheDirectoryName, CredentialCacheDirName));
        }

        private void InitializeForDirectory(string directory)
        {
            JsonCacheDirectory = directory;
            JsonCacheFilePath = Path.Combine(directory, CredentialCacheFileName);
            JsonCacheLockPath = Path.Combine(directory, CredentialCacheLockName);
            s_logger.Info($"Setting the json credential cache path to {JsonCacheLockPath}");
        }
    }
}
