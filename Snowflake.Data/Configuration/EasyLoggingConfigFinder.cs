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

        private readonly FileOperations _fileOperations;
        private readonly IEnvironmentFacade _environmentFacade;

        public static readonly EasyLoggingConfigFinder Instance = new EasyLoggingConfigFinder(FileOperations.Instance, EnvironmentFacade.Instance);

        internal EasyLoggingConfigFinder(FileOperations fileOperations, IEnvironmentFacade environmentFacade)
        {
            _fileOperations = fileOperations;
            _environmentFacade = environmentFacade;
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
            return configFilePath;
        }

        private string GetFilePathEnvironmentVariable()
        {
            var filePath = _environmentFacade.GetString(EnvVars.ClientConfigFile);
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

        private string GetHomeDirectory() => HomeDirectoryProvider.HomeDirectory(_environmentFacade);

        private string GetFilePathFromDriverLocation() => SearchForConfigInDirectory(() => _environmentFacade.GetExecutionDirectory(), "driver");

        private string SearchForConfigInDirectory(Func<string> directoryProvider, string directoryDescription)
        {
            try
            {
                var directory = directoryProvider.Invoke();
                if (string.IsNullOrEmpty(directory))
                {
                    s_logger.Warn($"The {directoryDescription} directory could not be determined and will be skipped");
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
                s_logger.Info($"Using config file specified from {directoryDescription} directory: {filePath}");
                return filePath;
            }
            return null;
        }
    }
}
