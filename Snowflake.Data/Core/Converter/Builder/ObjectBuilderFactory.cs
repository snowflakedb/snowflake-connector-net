using System;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Converter.Builder
{
    internal static class ObjectBuilderFactory
    {
        public static IObjectBuilder Create(Type type, int fieldsCount, SnowflakeObjectConstructionMethod constructionMethod)
        {
            if (constructionMethod == SnowflakeObjectConstructionMethod.PROPERTIES_NAMES)
            {
                return new ObjectBuilderByPropertyNames(type);
            }
            if (constructionMethod == SnowflakeObjectConstructionMethod.PROPERTIES_ORDER)
            {
                return new ObjectBuilderByPropertyOrder(type);
            }
            if (constructionMethod == SnowflakeObjectConstructionMethod.CONSTRUCTOR)
            {
                return new ObjectBuilderByConstructor(type, fieldsCount);
            }
            throw new StructuredTypesReadingException("Unknown construction method");
        }
    }
}
