using System;
using System.IO;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{
    internal class SFCredentialManagerFileStorage
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerFileStorage>();

        internal const string CommonCacheDirectoryName = ".cache";

        internal const string CredentialCacheDirName = "snowflake";

        internal const string CredentialCacheFileName = "credential_cache_v1.json";

        internal const string CredentialCacheLockName = CredentialCacheFileName + ".lck";

        public string JsonCacheDirectory { get; private set; }

        public string JsonCacheFilePath { get; private set; }

        public string JsonCacheLockPath { get; private set; }

        public SFCredentialManagerFileStorage(IEnvironmentFacade environmentFacade)
        {
            var snowflakeEnvBasedDirectory = environmentFacade.GetString(EnvVars.TemporaryCredentialDir);
            if (!string.IsNullOrEmpty(snowflakeEnvBasedDirectory))
            {
                InitializeForDirectory(snowflakeEnvBasedDirectory);
                return;
            }

            var commonCacheEnvBasedDirectory = environmentFacade.GetString(EnvVars.CommonCacheDirectory);
            if (!string.IsNullOrEmpty(commonCacheEnvBasedDirectory))
            {
                InitializeForDirectory(Path.Combine(commonCacheEnvBasedDirectory, CredentialCacheDirName));
                return;
            }
            var homeBasedDirectory = HomeDirectoryProvider.HomeDirectory(environmentFacade);
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
