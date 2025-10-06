using Newtonsoft.Json;

namespace Snowflake.Data.Configuration
{
    internal class ClientConfigDotnet
    {
        [JsonProperty(PropertyName = "log_file_unix_permissions")]
        public string LogFileUnixPermissions { get; set; }
    }
}
