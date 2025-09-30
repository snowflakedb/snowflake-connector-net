using System;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal class ValuesExtractor
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ValuesExtractor>();

        public static int ExtractInt(Func<string> extractor, string valueName, int defaultValue)
        {
            string value = null;
            try
            {
                value = extractor();
            }
            catch (Exception ex)
            {
                s_logger.Error($"Error when reading {valueName}. Using default value: {defaultValue}", ex);
                return defaultValue;
            }
            if (string.IsNullOrEmpty(value))
            {
                s_logger.Debug($"Did not retrieve value from {valueName}. Using default value: {defaultValue}");
                return defaultValue;
            }
            if (!int.TryParse(value, out int intValue))
            {
                s_logger.Error($"Value retrieved from {valueName} is not integer. Using default value: {defaultValue}");
                return defaultValue;
            }
            return intValue;
        }
    }
}
