using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Snowflake.Data.Tests.Util
{
    // Deserializes any JSON value directly into its compact string representation,
    // so callers never need to handle JToken.
    internal sealed class RawJsonConverter : JsonConverter<string>
    {
        public override string ReadJson(JsonReader reader, Type objectType, string existingValue, bool hasExistingValue, JsonSerializer serializer)
            => JToken.Load(reader).ToString(Formatting.None);

        public override void WriteJson(JsonWriter writer, string value, JsonSerializer serializer)
            => throw new NotSupportedException();
    }
}
