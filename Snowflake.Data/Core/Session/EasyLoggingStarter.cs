/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.IO;
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
                s_logger.Warn("LogPath in client config not found. Using temporary directory as a default value");
                logPathOrDefault = Path.GetTempPath();
            }
            var pathWithDotnetSubdirectory = Path.Combine(logPathOrDefault, "dotnet");
            if (!_directoryOperations.Exists(pathWithDotnetSubdirectory))
            {
                _directoryOperations.CreateDirectory(pathWithDotnetSubdirectory);
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
