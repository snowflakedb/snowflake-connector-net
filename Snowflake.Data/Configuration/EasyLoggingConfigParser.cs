/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Snowflake.Data.Log;

namespace Snowflake.Data.Configuration
{
    using System.Text.Json;
    using System.Text.Json.Serialization;

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
                var config = System.Text.Json.JsonSerializer.Deserialize<ClientConfig>(fileContent);
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
                .Select(property => property.GetCustomAttribute<JsonPropertyNameAttribute>().Name)
                .ToList();

            using (JsonDocument document = JsonDocument.Parse(fileContent))
            {
                if (document.RootElement.TryGetProperty("common", out JsonElement commonElement))
                {
                    foreach (JsonProperty property in commonElement.EnumerateObject())
                    {
                        if (!knownProperties.Contains(property.Name, StringComparer.OrdinalIgnoreCase))
                        {
                            s_logger.Warn($"Unknown field from config: {property.Name}");
                        }
                    }
                }
            }
        }
    }
}
