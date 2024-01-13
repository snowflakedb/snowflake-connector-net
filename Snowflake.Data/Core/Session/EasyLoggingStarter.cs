/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
using Snowflake.Data.Client;
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

        private readonly DirectoryOperations _directoryOperations;

        private readonly object _lockForExclusiveInit = new object();
        
        private EasyLoggingInitTrialParameters _initTrialParameters = null;

        public static readonly EasyLoggingStarter Instance = new EasyLoggingStarter(EasyLoggingConfigProvider.Instance,
            EasyLoggerManager.Instance, DirectoryOperations.Instance);
        
        internal EasyLoggingStarter(
            EasyLoggingConfigProvider easyLoggingConfigProvider,
            EasyLoggerManager easyLoggerManager,
            DirectoryOperations directoryOperations)
        {
            _easyLoggingConfigProvider = easyLoggingConfigProvider;
            _easyLoggerManager = easyLoggerManager;
            _directoryOperations = directoryOperations;
        }

        internal EasyLoggingStarter()
        {
        }

        public virtual void Init(string configFilePathFromConnectionString)
        {
            if (string.IsNullOrEmpty(configFilePathFromConnectionString))
            {
                s_logger.Info($"Attempting to enable easy logging without a config file specified from connection string");
            }
            else
            {
                s_logger.Info($"Attempting to enable easy logging using config file specified from connection string: {configFilePathFromConnectionString}");
            }

            lock (_lockForExclusiveInit)
            {
                if (!AllowedToInitialize(configFilePathFromConnectionString))
                {
                    return;
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
                logPathOrDefault = EnvironmentOperations.Instance.GetFolderPath(Environment.SpecialFolder.UserProfile);
                if (string.IsNullOrEmpty(logPathOrDefault))
                {
                    throw new SnowflakeDbException(
                        SFError.INTERNAL_ERROR,
                        "No log path found for easy logging. Home directory is not configured and log path is not provided.");
                }
            }
            var pathWithDotnetSubdirectory = Path.Combine(logPathOrDefault, "dotnet");
            if (!_directoryOperations.Exists(pathWithDotnetSubdirectory))
            {
                _directoryOperations.CreateDirectory(pathWithDotnetSubdirectory);

                if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var umask = EasyLoggerUtil.AllPermissions - int.Parse(EasyLoggerUtil.CallBash("umask"));
                    string dirPermissions = EasyLoggerUtil.CallBash($"stat -c '%a' {pathWithDotnetSubdirectory}");
                    if (int.Parse(dirPermissions) > umask)
                    {
                        EasyLoggerUtil.CallBash($"chmod -R {EasyLoggerUtil.AllUserPermissions} {pathWithDotnetSubdirectory}");
                    }
                    if (int.Parse(dirPermissions) != EasyLoggerUtil.AllUserPermissions)
                    {
                        s_logger.Warn($"Access permission for the logs directory is {dirPermissions}");
                    }
                }
            }

            return pathWithDotnetSubdirectory;
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
