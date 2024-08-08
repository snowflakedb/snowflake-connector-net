using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.Client
{
    [SnowflakeObject(ConstructionMethod = SnowflakeObjectConstructionMethod.PROPERTIES_ORDER)]
    public class AnnotatedClassForPropertiesOrderConstruction
    {
        public string StringValue { get; set; }
        [SnowflakeColumn(IgnoreForPropertyOrder = true)]
        public int? IgnoredValue { get; set; }
        public int IntegerValue { get; set; }
    }
}
