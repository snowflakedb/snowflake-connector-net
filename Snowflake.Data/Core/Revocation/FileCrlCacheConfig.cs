using System;
using System.IO;
using System.Runtime.InteropServices;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Revocation
{
    internal class FileCrlCacheConfig
    {
        public string DirectoryPath { get; set; }

        public bool IsWindows { get; set; }

        public long UnixUserId { get; set; }

        public long UnixGroupId { get; set; }

        public FileCrlCacheConfig(EnvironmentOperations environmentOperations, UnixOperations unixOperations)
        {
            var homeDirectory = HomeDirectoryProvider.HomeDirectory(environmentOperations);
            if (string.IsNullOrEmpty(homeDirectory))
            {
                throw new Exception("Could not determine home directory");
            }
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                IsWindows = true;
                UnixUserId = 0;
                UnixGroupId = 0;
                DirectoryPath = Path.Combine(homeDirectory, "AppData", "Local", "Snowflake", "Caches", "crls");
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                IsWindows = false;
                UnixUserId = unixOperations.GetCurrentUserId();
                UnixGroupId = unixOperations.GetCurrentGroupId();
                DirectoryPath = Path.Combine(homeDirectory, "Library", "Caches", "Snowflake", "crls");
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                IsWindows = false;
                UnixUserId = unixOperations.GetCurrentUserId();
                UnixGroupId = unixOperations.GetCurrentGroupId();
                DirectoryPath = Path.Combine(homeDirectory, ".cache", "snowflake", "crls");
            }
            else
            {
                throw new Exception("Unsupported platform. Could not determine a directory to store crl cache.");
            }
        }

        internal FileCrlCacheConfig(string directoryPath, bool isWindows, long unixUserId, long unixGroupId)
        {
            DirectoryPath = directoryPath;
            IsWindows = isWindows;
            UnixUserId = unixUserId;
            UnixGroupId = unixGroupId;
        }
    }
}
