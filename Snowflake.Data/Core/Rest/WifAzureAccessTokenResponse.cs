using Newtonsoft.Json;

namespace Snowflake.Data.Core.Rest
{
    internal class WifAzureAccessTokenResponse
    {
        [JsonProperty(PropertyName = "access_token", NullValueHandling = NullValueHandling.Ignore)]
        public string AccessToken;
    }
}
