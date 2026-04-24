using System.Collections.Generic;
using Newtonsoft.Json;

namespace Snowflake.Data.Tests.Util
{
    internal sealed class MappingFile
    {
        [JsonProperty("mappings")]
        public List<Mapping> Mappings { get; set; }
    }
}
