using Newtonsoft.Json;

namespace Snowflake.Data.Tests.Util
{
    internal sealed class Mapping
    {
        [JsonProperty("request")]
        public MappingRequest Request { get; set; }

        [JsonProperty("response")]
        public MappingResponse Response { get; set; }

        [JsonProperty("scenarioName")]
        public string ScenarioName { get; set; }

        [JsonProperty("requiredScenarioState")]
        public string RequiredScenarioState { get; set; }

        [JsonProperty("newScenarioState")]
        public string NewScenarioState { get; set; }
    }
}
