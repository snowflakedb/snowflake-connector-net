using Newtonsoft.Json;

namespace Snowflake.Data.Tests.Util.WiremockModels
{
    internal sealed class Mapping
    {
        [JsonProperty("request")]
        public MappingRequest Request { get; set; }

        [JsonProperty("response")]
        public MappingResponse Response { get; set; }

        [JsonProperty("scenarioName")]
        public string ScenarioName { get; set; }

        [JsonProperty("whenStateIs")]
        public string WhenStateIs { get; set; }

        [JsonProperty("setStateTo")]
        public string SetStateTo { get; set; }
    }
}
