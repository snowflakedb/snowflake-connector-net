using System.Net.Http;

namespace Snowflake.Data.Core
{
    /// <summary>
    /// Extension methods for <see cref="HttpRequestMessage"/> that abstract away the
    /// difference between <c>Properties</c> (netstandard2.0 / .NET Framework) and
    /// <c>Options</c> (.NET 5+), avoiding the CS0618 obsolete-member warning.
    /// </summary>
    internal static class HttpRequestMessageExtensions
    {
        internal static void SetOption(this HttpRequestMessage message, string key, object value)
        {
#if NET5_0_OR_GREATER
            message.Options.Set(new HttpRequestOptionsKey<object>(key), value);
#else
            message.Properties[key] = value;
#endif
        }

        internal static object GetOption(this HttpRequestMessage message, string key)
        {
#if NET5_0_OR_GREATER
            message.Options.TryGetValue(new HttpRequestOptionsKey<object>(key), out var value);
            return value;
#else
            return message.Properties[key];
#endif
        }
    }
}
