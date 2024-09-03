using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.Client
{
    [SnowflakeObject(ConstructionMethod = SnowflakeObjectConstructionMethod.PROPERTIES_NAMES)]
    public class AnnotatedClassForPropertiesNamesConstruction
    {
        [SnowflakeColumn(Name = "x")]
        public string StringValue { get; set; }
        public int? IgnoredValue { get; set; }
        public int IntegerValue { get; set; }
    }
}
