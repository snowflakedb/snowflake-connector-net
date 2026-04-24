using System.Collections.Generic;
using Newtonsoft.Json;

namespace Snowflake.Data.Tests.Util.WiremockModels;

internal sealed class MappingResponse
{
    [JsonProperty("status")]
    public int? Status { get; set; }

    [JsonProperty("headers")]
    public Dictionary<string, string> Headers { get; set; }

    [JsonProperty("jsonBody")]
    [JsonConverter(typeof(RawJsonConverter))]
    public string JsonBody { get; set; }

    [JsonProperty("body")]
    public string Body { get; set; }

    [JsonProperty("fixedDelayMilliseconds")]
    public int? FixedDelayMilliseconds { get; set; }
}
