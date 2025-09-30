using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Mono.Unix;
using System.Security;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigParser
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EasyLoggingConfigParser>();

        private readonly UnixOperations _unixOperations;

        public static readonly EasyLoggingConfigParser Instance = new EasyLoggingConfigParser(UnixOperations.Instance);

        internal EasyLoggingConfigParser(UnixOperations unixOperations)
        {
            _unixOperations = unixOperations;
        }

        internal EasyLoggingConfigParser() : this(UnixOperations.Instance) { }

        public virtual ClientConfig Parse(string filePath)
        {
            var configFile = TryToReadFile(filePath);
            return configFile == null ? null : TryToParseFile(configFile);
        }

        private string TryToReadFile(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
            {
                return null;
            }
            try
            {
                return FileOperations.Instance.ReadAllText(filePath, CheckIfValidPermissions);
            }
            catch (Exception e)
            {
                var errorMessage = $"Finding easy logging configuration failed: {e.Message}";
                s_logger.Error(errorMessage, e);
                throw new Exception(errorMessage);
            }
        }

        private ClientConfig TryToParseFile(string fileContent)
        {
            try
            {
                var config = JsonConvert.DeserializeObject<ClientConfig>(fileContent);
                Validate(config);
                CheckForUnknownFields(fileContent);
                return config;
            }
            catch (Exception e)
            {
                var errorMessage = "Parsing easy logging configuration failed";
                s_logger.Error(errorMessage, e);
                throw new Exception(errorMessage);
            }
        }

        private void Validate(ClientConfig config)
        {
            if (config.CommonProps.LogLevel != null)
            {
                EasyLoggingLogLevelExtensions.From(config.CommonProps.LogLevel);
            }
        }

        private void CheckForUnknownFields(string fileContent)
        {
            // Parse the specified config file and get the key-value pairs from the "common" section
            List<string> knownProperties = typeof(ClientConfigCommonProps).GetProperties()
                .Select(property => property.GetCustomAttribute<JsonPropertyAttribute>().PropertyName)
                .ToList();

            JObject.Parse(fileContent).GetValue("common", StringComparison.OrdinalIgnoreCase)?
                .Cast<JProperty>()
                .Where(property => !knownProperties.Contains(property.Name, StringComparer.OrdinalIgnoreCase))
                .ToList()
                .ForEach(unknownKey => s_logger.Warn($"Unknown field from config: {unknownKey.Name}"));
        }

        private void CheckIfValidPermissions(UnixStream stream)
        {
            // Check user ownership of file
            if (stream.OwnerUserId != _unixOperations.GetCurrentUserId())
            {
                var errorMessage = $"Error due to user not having ownership of the config file";
                s_logger.Error(errorMessage);
                throw new SecurityException(errorMessage);
            }

            // Check group ownership of file
            if (stream.OwnerGroupId != _unixOperations.GetCurrentGroupId())
            {
                var errorMessage = $"Error due to group not having ownership of the config file";
                s_logger.Error(errorMessage);
                throw new SecurityException(errorMessage);
            }

            // Check if others have permissions to modify the file and fail if so
            if (_unixOperations.CheckFileHasAnyOfPermissions(stream.FileAccessPermissions, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite))
            {
                var errorMessage = $"Error due to other users having permission to modify the config file";
                s_logger.Error(errorMessage);
                throw new SecurityException(errorMessage);
            }
        }
    }
}
