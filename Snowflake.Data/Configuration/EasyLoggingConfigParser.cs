using System;
using System.IO;
using Newtonsoft.Json;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfigParser
    {
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
                Console.WriteLine("Finding easy logging configuration failed");
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
                Console.WriteLine("Parsing easy logging configuration failed");
                return null;
            }         
        }

        private void Validate(EasyLoggingConfig config)
        {
            EasyLoggingLogLevelExtensions.From(config.CommonProps.LogLevel);
        }
    }
}
