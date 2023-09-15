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
        
        private EasyLoggingInitParameters _initParameters = null;

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
                    _initParameters = EasyLoggingInitParameters.CreateWhenConfigNotFound(configFilePathFromConnectionString);
                    return;
                }
                var logLevel = GetLogLevel(config.CommonProps.LogLevel);
                var logPath = GetLogPath(config.CommonProps.LogPath);
                _easyLoggerManager.ReconfigureEasyLogging(logLevel, logPath);
                _initParameters = EasyLoggingInitParameters.CreateWhenConfigFound(configFilePathFromConnectionString, logLevel, logPath);
            }
        }

        private bool AllowedToInitialize(string configFilePathFromConnectionString)
        {
            var isAllowed = NeverTriedToInitialize() ||
                   (TriedToInitializeWithoutConfigFileFromConnectionString() &&
                    !string.IsNullOrEmpty(configFilePathFromConnectionString));
            if (!isAllowed)
            {
                WarnWhyNotAllowed(configFilePathFromConnectionString);
            }

            return isAllowed;
        }
        
        private bool NeverTriedToInitialize()
        {
            return _initParameters == null;
        }

        private bool TriedToInitializeWithoutConfigFileFromConnectionString()
        {
            return _initParameters != null && _initParameters.HasNoConfigFilePathFromConnectionString();
        }

        private void WarnWhyNotAllowed(string configFilePath)
        {
            var isDifferentConfigPath = _initParameters.ConfigFilePathFromConnectionString != null
                   && configFilePath != null
                   && _initParameters.ConfigFilePathFromConnectionString != configFilePath;
            if (isDifferentConfigPath)
            {
                s_logger.Warn($"Easy logging will not be configured for CLIENT_CONFIG_FILE={configFilePath} because it was previously configured for a different client config");
            }
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

    internal class EasyLoggingInitParameters
    {
        public string ConfigFilePathFromConnectionString { get; }

        private readonly EasyLoggingLogLevel? _logLevel;

        private readonly string _logPath;

        private EasyLoggingInitParameters(
            string configFilePathFromConnectionString,
            EasyLoggingLogLevel? logLevel,
            string logPath)
        {
            ConfigFilePathFromConnectionString = configFilePathFromConnectionString;
            _logLevel = logLevel;
            _logPath = logPath;
        }

        public static EasyLoggingInitParameters CreateWhenConfigNotFound(string configFilePathFromConnectionString) =>
            new EasyLoggingInitParameters(configFilePathFromConnectionString, null, null);
        
        public static EasyLoggingInitParameters CreateWhenConfigFound(string configFilePathFromConnectionString, EasyLoggingLogLevel logLevel, string logPath) =>
            new EasyLoggingInitParameters(configFilePathFromConnectionString, logLevel, logPath);

        public bool HasNoConfigFilePathFromConnectionString()
        {
            return string.IsNullOrEmpty(ConfigFilePathFromConnectionString);
        }
    }
}
