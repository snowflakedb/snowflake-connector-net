/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigFinder
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EasyLoggingConfigFinder>();
        
        internal const string ClientConfigFileName = "sf_client_config.json";
        internal const string ClientConfigEnvironmentName = "SF_CLIENT_CONFIG_FILE";

        private readonly FileOperations _fileOperations;
        private readonly EnvironmentOperations _environmentOperations;
        
        public static readonly EasyLoggingConfigFinder Instance = new EasyLoggingConfigFinder(FileOperations.Instance, EnvironmentOperations.Instance);

        internal EasyLoggingConfigFinder(FileOperations fileOperations, EnvironmentOperations environmentOperations)
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
        
        private string GetFilePathFromTempDirectory() => SearchForConfigInDirectory(Path.GetTempPath, "temp");

        private string GetFilePathFromHomeDirectory() => SearchForConfigInDirectory(GetHomeDirectory, "home");
        
        private string GetFilePathFromInputParameter(string filePath) => string.IsNullOrEmpty(filePath) ? null : filePath;

        private string GetHomeDirectory() =>_environmentOperations.GetFolderPath(Environment.SpecialFolder.UserProfile);

        private string GetFilePathFromDriverLocation() => SearchForConfigInDirectory(() => ".", "driver");

        private string SearchForConfigInDirectory(Func<string> directoryProvider, string directoryDescription)
        {
            try
            {
                var directory = directoryProvider.Invoke();
                if (string.IsNullOrEmpty(directory))
                {
                    return null;
                }

                var filePath = Path.Combine(directory, ClientConfigFileName);
                return OnlyIfFileExists(filePath);
            }
            catch (Exception e)
            {
                s_logger.Error($"Error while searching for the client config in {directoryDescription} directory: {e}");
                return null;
            }
        }

        private string OnlyIfFileExists(string filePath) => _fileOperations.Exists(filePath) ? filePath : null;
    }
}
