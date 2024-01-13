/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO;
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
                CheckForUnknownFields(fileContent, config);
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

        private void CheckForUnknownFields(string fileContent, ClientConfig config)
        {
            // Parse the specified config file and get the key-value pairs from the "common" section
            JObject obj = (JObject)(JObject.Parse(fileContent).First.First);
            bool isUnknownField = true;
            foreach (var keyValuePair in obj)
            {
                foreach(var property in config.CommonProps.GetType().GetProperties())
                {
                    var jsonPropertyAttribute = property.GetCustomAttribute<JsonPropertyAttribute>();
                    if (keyValuePair.Key.Equals(jsonPropertyAttribute.PropertyName))
                    {
                        isUnknownField = false;
                        break;
                    }
                }
                if (isUnknownField)
                {
                    s_logger.Warn($"Unknown field from config: {keyValuePair.Key}");
                }

                isUnknownField = true;
            }
        }
    }
}
