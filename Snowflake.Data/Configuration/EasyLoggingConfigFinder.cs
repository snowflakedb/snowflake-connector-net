/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix;
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
        private readonly UnixFileOperations _unixFileOperations;
        private readonly DirectoryOperations _directoryOperations;
        private readonly EnvironmentOperations _environmentOperations;
        
        public static readonly EasyLoggingConfigFinder Instance = new EasyLoggingConfigFinder(FileOperations.Instance, UnixFileOperations.Instance, DirectoryOperations.Instance, EnvironmentOperations.Instance);

        internal EasyLoggingConfigFinder(FileOperations fileOperations, UnixFileOperations unixFileOperations, DirectoryOperations directoryOperations, EnvironmentOperations environmentOperations)
        {
            _fileOperations = fileOperations;
            _unixFileOperations = unixFileOperations;
            _directoryOperations = directoryOperations;
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
            s_logger.Info($"Using config file specified from {inputDescription}: {filePath}");
            return filePath;
        }

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
                if (!_directoryOperations.Exists(directory))
                {
                    s_logger.Warn($"Searching for a config file in the {directoryDescription} directory that does not exist: {directory}");
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
                s_logger.Info($"Using config file specified from {directoryDescription} directory: {filePath}");
                return filePath;
            }
            return null;
        }

        private void CheckIfValidPermissions(string filePath)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return;

            // Check if others have permissions to modify the file and fail if so
            _unixFileOperations.SetUnixFileInfo(filePath);
            if (_unixFileOperations.HasFlag(FileAccessPermissions.GroupWrite) ||
                _unixFileOperations.HasFlag(FileAccessPermissions.OtherWrite))
            {
                var errorMessage = $"Error due to other users having permission to modify the config file: {filePath}";
                s_logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }
    }
}
