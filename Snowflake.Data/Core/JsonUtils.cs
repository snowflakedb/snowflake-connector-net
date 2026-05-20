using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Snowflake.Data.Core
{
    internal static class JsonUtils
    {
        /// <summary>
        /// Default serialization settings for JSON serialization and deserialization.
        /// This is to avoid issues when changes are made system wide to the default and keep
        /// our settings locals.
        /// </summary>
        public static readonly JsonSerializerSettings JsonSettings = new()
        {
            ContractResolver = new DefaultContractResolver
            {
                NamingStrategy = new DefaultNamingStrategy()
            },
            TypeNameHandling = TypeNameHandling.None
        };

        public static readonly JsonSerializer Serializer = JsonSerializer.Create(JsonSettings);
    }
}
