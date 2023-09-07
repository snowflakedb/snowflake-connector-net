using System;
using System.IO;
using Newtonsoft.Json;
using Snowflake.Data.Log;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigParser
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EasyLoggingConfigParser>();
        
        public virtual EasyLoggingConfig Parse(string filePath)
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
                s_logger.Error("Finding easy logging configuration failed");
                return null;
            }  
        }

        private EasyLoggingConfig TryToParseFile(string fileContent)
        {
            try {
                var config = JsonConvert.DeserializeObject<EasyLoggingConfig>(fileContent);
                Validate(config);
                return config;
            }
            catch (Exception e)
            {
                s_logger.Error("Parsing easy logging configuration failed");
                return null;
            }
        }

        private void Validate(EasyLoggingConfig config)
        {
            EasyLoggingLogLevelExtensions.From(config.CommonProps.LogLevel);
        }
    }
}
