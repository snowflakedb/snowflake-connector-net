/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Log;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigParser
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EasyLoggingConfigParser>();

        public static readonly EasyLoggingConfigParser Instance = new EasyLoggingConfigParser();

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
                return File.ReadAllText(filePath);
            }
            catch (Exception e)
            {
                var errorMessage = "Finding easy logging configuration failed";
                s_logger.Error(errorMessage, e);
                throw new Exception(errorMessage);
            }
        }

        private ClientConfig TryToParseFile(string fileContent)
        {
            try {
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
    }
}
