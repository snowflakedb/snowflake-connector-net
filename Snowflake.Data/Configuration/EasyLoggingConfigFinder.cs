/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigFinder
    {
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
        
        private string GetFilePathFromTempDirectory() => SearchForConfigInDirectory(Path.GetTempPath());

        private string GetFilePathFromHomeDirectory() => SearchForConfigInDirectory(GetHomeDirectory());
        
        private string GetFilePathFromInputParameter(string filePath) => string.IsNullOrEmpty(filePath) ? null : filePath;

        private string GetHomeDirectory() =>_environmentOperations.GetFolderPath(Environment.SpecialFolder.UserProfile);

        private string GetFilePathFromDriverLocation() => SearchForConfigInDirectory(".");

        private string SearchForConfigInDirectory(string directory)
        {
            if (string.IsNullOrEmpty(directory))
            {
                return null;
            }
            var filePath = Path.Combine(directory, ClientConfigFileName);
            return OnlyIfFileExists(filePath);            
        }

        private string OnlyIfFileExists(string filePath) => _fileOperations.Exists(filePath) ? filePath : null;
    }
}
