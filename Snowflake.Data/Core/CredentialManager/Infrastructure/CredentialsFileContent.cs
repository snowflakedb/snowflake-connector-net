using Newtonsoft.Json;
using KeyTokenDict = System.Collections.Generic.Dictionary<string, string>;

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{
    internal class CredentialsFileContent
    {
        [JsonProperty(PropertyName = "tokens")]
        internal KeyTokenDict Tokens { get; set; }
    }
}
