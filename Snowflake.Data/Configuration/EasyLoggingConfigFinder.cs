using System;
using System.IO;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigFinder
    {
        internal const string ClientConfigFileName = "sf_client_config.json";
        internal const string ClientConfigEnvironmentName = "SF_CLIENT_CONFIG_FILE";
        internal const string UnixHomeEnvName = "HOME";
        internal const string WindowsHomePathExtractionTemplate = "%HOMEDRIVE%%HOMEPATH%";
        
        public virtual string FindConfigFilePath(string configFilePathFromConnectionString)
        {
            return GetFilePathFromInputParameter(configFilePathFromConnectionString)
                    ?? GetFilePathEnvironmentVariable()
                    ?? GetFilePathFromDriverLocation()
                    ?? GetFilePathFromHomeDirectory()
                    ?? GetFilePathFromTempDirectory();
        }
        
        internal virtual string GetFilePathEnvironmentVariable()
        {
            var filePath = Environment.GetEnvironmentVariable(ClientConfigEnvironmentName);
            return GetFilePathFromInputParameter(filePath);
        }
        
        internal virtual string GetFilePathFromTempDirectory()
        {
            var tempDirectory = Path.GetTempPath();
            if (string.IsNullOrEmpty(tempDirectory))
            {
                return null;
            }
            var tempFilePath = Path.Combine(tempDirectory, ClientConfigFileName);
            return File.Exists(tempFilePath) ? tempFilePath : null;
        }
        
        internal virtual string GetFilePathFromHomeDirectory()
        {
            var homeDirectory = GetHomeDirectory();
            if (string.IsNullOrEmpty(homeDirectory))
            {
                return null;
            }
            var homeFilePath = Path.Combine(homeDirectory, ClientConfigFileName);
            return File.Exists(homeFilePath) ? homeFilePath : null;
        }
        
        private string GetFilePathFromInputParameter(string filePath)
        {
            return string.IsNullOrEmpty(filePath) ? null : filePath;
        }

        private string GetHomeDirectory()
        {
            var platform = Environment.OSVersion.Platform;
            if (platform == PlatformID.Unix || platform == PlatformID.MacOSX)
            {
                return Environment.GetEnvironmentVariable(UnixHomeEnvName);
            }

            return Environment.ExpandEnvironmentVariables(WindowsHomePathExtractionTemplate);
        }

        internal virtual string GetFilePathFromDriverLocation()
        {
            var filePath = Path.Combine(".", ClientConfigFileName);
            return File.Exists(filePath) ? filePath : null;
        }
    }
}
