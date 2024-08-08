using System;

namespace Snowflake.Data.Client
{
    public class SnowflakeColumn : Attribute
    {
        public string Name { get; set; } = null;
        public bool IgnoreForPropertyOrder { get; set; } = false;
    }
}
