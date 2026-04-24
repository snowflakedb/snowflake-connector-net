using Newtonsoft.Json;

namespace Snowflake.Data.Tests.Util.WiremockModels;

internal sealed class BodyPattern
{
    [JsonProperty("matchesJsonPath")]
    public string MatchesJsonPath { get; set; }

    [JsonProperty("contains")]
    public string Contains { get; set; }

    [JsonProperty("equalToJson")]
    [JsonConverter(typeof(RawJsonConverter))]
    public string EqualToJson { get; set; }
}
