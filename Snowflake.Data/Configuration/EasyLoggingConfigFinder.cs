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

        private readonly FileOperations _fileOperations;
        private readonly EnvironmentOperations _environmentOperations;

        public EasyLoggingConfigFinder(FileOperations fileOperations, EnvironmentOperations environmentOperations)
        {
            _fileOperations = fileOperations;
            _environmentOperations = environmentOperations;
        }

        internal EasyLoggingConfigFinder()
        {
        }

        public virtual string FindConfigFilePath(string configFilePathFromConnectionString)
        {
            return GetFilePathFromInputParameter(configFilePathFromConnectionString)
                    ?? GetFilePathEnvironmentVariable()
                    ?? GetFilePathFromDriverLocation()
                    ?? GetFilePathFromHomeDirectory()
                    ?? GetFilePathFromTempDirectory();
        }
        
        private string GetFilePathEnvironmentVariable()
        {
            var filePath = _environmentOperations.GetEnvironmentVariable(ClientConfigEnvironmentName);
            return GetFilePathFromInputParameter(filePath);
        }
        
        private string GetFilePathFromTempDirectory()
        {
            var tempDirectory = Path.GetTempPath();
            if (string.IsNullOrEmpty(tempDirectory))
            {
                return null;
            }
            var tempFilePath = Path.Combine(tempDirectory, ClientConfigFileName);
            return OnlyIfFileExists(tempFilePath);
        }
        
        private string GetFilePathFromHomeDirectory()
        {
            var homeDirectory = GetHomeDirectory();
            if (string.IsNullOrEmpty(homeDirectory))
            {
                return null;
            }
            var homeFilePath = Path.Combine(homeDirectory, ClientConfigFileName);
            return OnlyIfFileExists(homeFilePath);
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
                return _environmentOperations.GetEnvironmentVariable(UnixHomeEnvName);
            }
            return _environmentOperations.ExpandEnvironmentVariables(WindowsHomePathExtractionTemplate);
        }

        private string GetFilePathFromDriverLocation()
        {
            var filePath = Path.Combine(".", ClientConfigFileName);
            return OnlyIfFileExists(filePath);
        }

        private string OnlyIfFileExists(string filePath) => _fileOperations.Exists(filePath) ? filePath : null;
    }
}
