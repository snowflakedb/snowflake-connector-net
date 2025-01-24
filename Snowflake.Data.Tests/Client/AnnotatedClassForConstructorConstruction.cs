using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.Client
{
    [SnowflakeObject(ConstructionMethod = SnowflakeObjectConstructionMethod.CONSTRUCTOR)]
    public class AnnotatedClassForConstructorConstruction
    {
        public string StringValue { get; set; }
        public int? IgnoredValue { get; set; }
        public int IntegerValue { get; set; }

        public AnnotatedClassForConstructorConstruction()
        {
        }

        public AnnotatedClassForConstructorConstruction(string stringValue, int integerValue)
        {
            StringValue = stringValue;
            IntegerValue = integerValue;
        }
    }
}
