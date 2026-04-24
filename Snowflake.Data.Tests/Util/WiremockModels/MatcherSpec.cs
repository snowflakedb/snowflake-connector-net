using Newtonsoft.Json;

namespace Snowflake.Data.Tests.Util
{
    internal sealed class MatcherSpec
    {
        [JsonProperty("equalTo")]
        public string EqualTo { get; set; }

        [JsonProperty("contains")]
        public string Contains { get; set; }

        [JsonProperty("matches")]
        public string Matches { get; set; }
    }
}
