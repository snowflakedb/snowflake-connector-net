using System.Collections.Generic;
using Newtonsoft.Json;

namespace Snowflake.Data.Tests.Util
{
    internal sealed class MappingRequest
    {
        [JsonProperty("urlPathPattern")]
        public string UrlPathPattern { get; set; }

        [JsonProperty("urlPattern")]
        public string UrlPattern { get; set; }

        [JsonProperty("method")]
        public string Method { get; set; }

        [JsonProperty("headers")]
        public Dictionary<string, MatcherSpec> Headers { get; set; }

        [JsonProperty("queryParameters")]
        public Dictionary<string, MatcherSpec> QueryParameters { get; set; }

        [JsonProperty("bodyPatterns")]
        public List<BodyPattern> BodyPatterns { get; set; }
    }
}
