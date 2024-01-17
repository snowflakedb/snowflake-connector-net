/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
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
            var configFilePath = GetFilePathFromInputParameter(configFilePathFromConnectionString, "connection string")
                              ?? GetFilePathEnvironmentVariable()
                              ?? GetFilePathFromDriverLocation()
                              ?? GetFilePathFromHomeDirectory();
            if (configFilePath != null)
            {
                CheckIfValidPermissions(configFilePath);
            }
            return configFilePath;
        }
        
        private string GetFilePathEnvironmentVariable()
        {
            var filePath = _environmentOperations.GetEnvironmentVariable(ClientConfigEnvironmentName);
            return GetFilePathFromInputParameter(filePath, "environment variable");
        }

        private string GetFilePathFromHomeDirectory() => SearchForConfigInDirectory(GetHomeDirectory, "home");

        private string GetFilePathFromInputParameter(string filePath, string inputDescription)
        {
            if (string.IsNullOrEmpty(filePath))
            {
                return null;
            }
            s_logger.Info($"Using config file specified from {inputDescription}");
            return filePath;
        }

        private string GetHomeDirectory() =>_environmentOperations.GetFolderPath(Environment.SpecialFolder.UserProfile);

        private string GetFilePathFromDriverLocation() => SearchForConfigInDirectory(() => ".", "driver");

        private string SearchForConfigInDirectory(Func<string> directoryProvider, string directoryDescription)
        {
            try
            {
                var directory = directoryProvider.Invoke();
                if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory))
                {
                    return null;
                }

                var filePath = Path.Combine(directory, ClientConfigFileName);
                return OnlyIfFileExists(filePath, directoryDescription);
            }
            catch (Exception e)
            {
                s_logger.Error($"Error while searching for the client config in {directoryDescription} directory: {e}");
                return null;
            }
        }

        private string OnlyIfFileExists(string filePath, string directoryDescription)
        {
            if (_fileOperations.Exists(filePath))
            {
                s_logger.Info($"Using config file specified from {directoryDescription} directory");
                return filePath;
            }
            return null;
        }

        private void CheckIfValidPermissions(string filePath)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return;

            // Check if others have permissions to modify the file and fail if so
            int filePermissions;
            string commandParameters = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "-c '%a'" : "-f %A";
            bool isParsed = int.TryParse(EasyLoggerUtil.CallBash($"stat {commandParameters} {filePath}"), out filePermissions);
            if (isParsed && filePermissions > EasyLoggerUtil.OnlyUserHasPermissionToWrite)
            {
                var errorMessage = "Error due to other users having permission to modify the config file";
                s_logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }
    }
}
