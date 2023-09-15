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
                var previousInitParameters = _initParameters;
                if (config == null)
                {
                    _initParameters = EasyLoggingInitParameters.WhenConfigNotFound(configFilePathFromConnectionString);
                    return;
                }
                var logLevel = GetLogLevel(config.CommonProps.LogLevel);
                var logPath = GetLogPath(config.CommonProps.LogPath);
                WarnWhenConfigureForTheSecondTime(previousInitParameters);
                _easyLoggerManager.ReconfigureEasyLogging(logLevel, logPath);
                _initParameters = EasyLoggingInitParameters.WhenConfigFound(configFilePathFromConnectionString, logLevel, logPath);
            }
        }

        private bool AllowedToInitialize(string configFilePathFromConnectionString)
        {
            return NeverTriedToInitialize() ||
                   (TriedToInitializedWithoutConfigFileFromConnectionString() &&
                    !string.IsNullOrEmpty(configFilePathFromConnectionString));
        }

        private bool NeverTriedToInitialize()
        {
            return _initParameters == null;
        }

        private bool TriedToInitializedWithoutConfigFileFromConnectionString()
        {
            return _initParameters != null && _initParameters.HasNoConfigFilePathFromConnectionString();
        }

        private void WarnWhenConfigureForTheSecondTime(EasyLoggingInitParameters previousInitParameters)
        {
            if (previousInitParameters == null)
            {
                return;
            }
            if (previousInitParameters.ShouldConfigureLogger && _initParameters.ShouldConfigureLogger)
            {
                s_logger.Warn("Easy logging will be configured once again because it's the first time where client config is provided in a connection string");
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
        private readonly string _configFilePathFromConnectionString;

        private readonly EasyLoggingLogLevel? _logLevel;

        private readonly string _logPath;
        
        public bool ShouldConfigureLogger { get; }

        private EasyLoggingInitParameters(
            string configFilePathFromConnectionString,
            EasyLoggingLogLevel? logLevel,
            string logPath,
            bool shouldConfigureLogger)
        {
            _configFilePathFromConnectionString = configFilePathFromConnectionString;
            _logLevel = logLevel;
            _logPath = logPath;
            ShouldConfigureLogger = shouldConfigureLogger;
        }

        public static EasyLoggingInitParameters WhenConfigNotFound(string configFilePathFromConnectionString) =>
            new EasyLoggingInitParameters(configFilePathFromConnectionString, null, null, false);
        
        public static EasyLoggingInitParameters WhenConfigFound(string configFilePathFromConnectionString, EasyLoggingLogLevel logLevel, string logPath) =>
            new EasyLoggingInitParameters(configFilePathFromConnectionString, logLevel, logPath, true);

        public bool HasNoConfigFilePathFromConnectionString()
        {
            return string.IsNullOrEmpty(_configFilePathFromConnectionString);
        }
    }
}
