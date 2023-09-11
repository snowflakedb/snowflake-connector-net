using Newtonsoft.Json;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingCommonProps
    {
        [JsonProperty(Required = Required.Always, PropertyName = "log_level")]
        public string LogLevel { get; set; }
        
        [JsonProperty(Required = Required.Always, PropertyName = "log_path")]
        public string LogPath { get; set; }
    }
}
