using Newtonsoft.Json;

namespace Snowflake.Data.Configuration
{
    internal class EasyLoggingConfig
    {
        [JsonProperty(Required = Required.Always, PropertyName = "common")]
        public EasyLoggingCommonProps CommonProps { get; set; }
    }
}
