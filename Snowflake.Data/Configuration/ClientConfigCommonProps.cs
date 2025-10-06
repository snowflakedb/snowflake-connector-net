using Newtonsoft.Json;

namespace Snowflake.Data.Configuration
{
    internal class ClientConfigCommonProps
    {
        [JsonProperty(PropertyName = "log_level")]
        public string LogLevel { get; set; }

        [JsonProperty(PropertyName = "log_path")]
        public string LogPath { get; set; }
    }
}
