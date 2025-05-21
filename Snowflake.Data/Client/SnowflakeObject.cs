using System;

namespace Snowflake.Data.Client
{
    public class SnowflakeObject : Attribute
    {
        public SnowflakeObjectConstructionMethod ConstructionMethod { get; set; } = SnowflakeObjectConstructionMethod.PROPERTIES_ORDER;
    }
}
