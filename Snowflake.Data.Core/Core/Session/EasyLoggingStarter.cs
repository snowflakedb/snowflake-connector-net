/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix;
using Mono.Unix.Native;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    internal class EasyLoggingStarter
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EasyLoggingStarter>();
        
        private readonly EasyLoggingConfigProvider _easyLoggingConfigProvider;

        private readonly EasyLoggerManager _easyLoggerManager;

        private readonly UnixOperations _unixOperations;

        private readonly DirectoryOperations _directoryOperations;

        private readonly EnvironmentOperations _environmentOperations;

        private readonly object _lockForExclusiveInit = new object();
        
        private EasyLoggingInitTrialParameters _initTrialParameters = null;

        public static readonly EasyLoggingStarter Instance = new EasyLoggingStarter(EasyLoggingConfigProvider.Instance,
            EasyLoggerManager.Instance, UnixOperations.Instance, DirectoryOperations.Instance, EnvironmentOperations.Instance);
        
        internal EasyLoggingStarter(
            EasyLoggingConfigProvider easyLoggingConfigProvider,
            EasyLoggerManager easyLoggerManager,
            UnixOperations unixOperations,
            DirectoryOperations directoryOperations,
            EnvironmentOperations environmentOperations)
        {
            _easyLoggingConfigProvider = easyLoggingConfigProvider;
            _easyLoggerManager = easyLoggerManager;
            _unixOperations = unixOperations;
            _directoryOperations = directoryOperations;
            _environmentOperations = environmentOperations;
        }

        internal EasyLoggingStarter()
        {
        }

        public virtual void Init(string configFilePathFromConnectionString)
        {
            lock (_lockForExclusiveInit)
            {
                if (!AllowedToInitialize(configFilePathFromConnectionString))
                {
                    return;
                }
                if (string.IsNullOrEmpty(configFilePathFromConnectionString))
                {
                    s_logger.Info($"Attempting to enable easy logging without a config file specified from connection string");
                }
                else
                {
                    s_logger.Info($"Attempting to enable easy logging using config file specified from connection string: {configFilePathFromConnectionString}");
                }
                var config = _easyLoggingConfigProvider.ProvideConfig(configFilePathFromConnectionString);
                if (config == null)
                {
                    _initTrialParameters = new EasyLoggingInitTrialParameters(configFilePathFromConnectionString);
                    return;
                }
                var logLevel = GetLogLevel(config.CommonProps.LogLevel);
                var logPath = GetLogPath(config.CommonProps.LogPath);
                s_logger.Info($"LogLevel set to {logLevel}");
                s_logger.Info($"LogPath set to {logPath}");
                _easyLoggerManager.ReconfigureEasyLogging(logLevel, logPath);
                _initTrialParameters = new EasyLoggingInitTrialParameters(configFilePathFromConnectionString);
            }
        }

        internal void Reset(EasyLoggingLogLevel logLevel)
        {
            lock (_lockForExclusiveInit)
            {
                _initTrialParameters = null;
                _easyLoggerManager.ResetEasyLogging(logLevel);
            }
        }
        
        private bool AllowedToInitialize(string configFilePathFromConnectionString)
        {
            var everTriedToInitialize = _initTrialParameters != null;
            var triedToInitializeWithoutConfigFile = everTriedToInitialize && !_initTrialParameters.IsConfigFilePathGiven();
            var isGivenConfigFilePath = !string.IsNullOrEmpty(configFilePathFromConnectionString);
            var isAllowedToInitialize = !everTriedToInitialize || (triedToInitializeWithoutConfigFile && isGivenConfigFilePath);
            if (!isAllowedToInitialize && _initTrialParameters.HasDifferentConfigPath(configFilePathFromConnectionString))
            {
                s_logger.Warn($"Easy logging will not be configured for CLIENT_CONFIG_FILE={configFilePathFromConnectionString} because it was previously configured for a different client config");
            }

            return isAllowedToInitialize;
        }

        private EasyLoggingLogLevel GetLogLevel(string logLevel)
        {
            if (string.IsNullOrEmpty(logLevel))
            {
                s_logger.Warn("LogLevel in client config not found. Using default value: OFF");
                return EasyLoggingLogLevel.Off;
            }
            return EasyLoggingLogLevelExtensions.From(logLevel);
        }

        private string GetLogPath(string logPath)
        {
            var logPathOrDefault = logPath;
            if (string.IsNullOrEmpty(logPath))
            {
                s_logger.Warn("LogPath in client config not found. Using home directory as a default value");
                logPathOrDefault = HomeDirectoryProvider.HomeDirectory(_environmentOperations);
                if (string.IsNullOrEmpty(logPathOrDefault))
                {
                    throw new Exception("No log path found for easy logging. Home directory is not configured and log path is not provided");
                }
            }
            var pathWithDotnetSubdirectory = Path.Combine(logPathOrDefault, "dotnet");
            if (!_directoryOperations.Exists(pathWithDotnetSubdirectory))
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    _directoryOperations.CreateDirectory(pathWithDotnetSubdirectory);
                }
                else
                {
                    if (!Directory.Exists(logPathOrDefault))
                    {
                        Directory.CreateDirectory(logPathOrDefault);
                    }
                    var createDirResult = _unixOperations.CreateDirectoryWithPermissions(pathWithDotnetSubdirectory,
                        FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR);
                    if (createDirResult != 0)
                    {
                        s_logger.Error($"Failed to create logs directory: {pathWithDotnetSubdirectory}");
                        throw new Exception("Failed to create logs directory");
                    }
                }
            }
            CheckDirPermissionsOnlyAllowUser(pathWithDotnetSubdirectory);

            return pathWithDotnetSubdirectory;
        }

        private void CheckDirPermissionsOnlyAllowUser(string dirPath)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return;

            var dirPermissions = _unixOperations.GetDirPermissions(dirPath);
            if (dirPermissions != FileAccessPermissions.UserReadWriteExecute)
            {
                s_logger.Warn($"Access permission for the logs directory is currently " +
                    $"{UnixFilePermissionsConverter.ConvertFileAccessPermissionsToInt(dirPermissions)} " +
                    $"and is potentially accessible to users other than the owner of the logs directory");
            }
        }
    }

    internal class EasyLoggingInitTrialParameters
    {
        private readonly string _configFilePathFromConnectionString;

        public EasyLoggingInitTrialParameters(
            string configFilePathFromConnectionString)
        {
            _configFilePathFromConnectionString = configFilePathFromConnectionString;
        }

        public bool IsConfigFilePathGiven()
        {
            return _configFilePathFromConnectionString != null;
        }
        
        public bool HasDifferentConfigPath(string configFilePath)
        {
            return IsConfigFilePathGiven()
                   && configFilePath != null
                   && _configFilePathFromConnectionString != configFilePath;
        }
    }
}
