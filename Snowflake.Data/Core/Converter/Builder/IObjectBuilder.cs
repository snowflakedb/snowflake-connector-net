using System;

namespace Snowflake.Data.Core.Converter.Builder
{
    internal interface IObjectBuilder
    {
        void BuildPart(object value);

        Type MoveNext(string sfPropertyName);

        object Build();
    }
}
