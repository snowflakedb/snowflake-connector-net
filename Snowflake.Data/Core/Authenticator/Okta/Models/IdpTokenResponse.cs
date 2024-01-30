using Newtonsoft.Json;

namespace Snowflake.Data.Core.Authenticator.Okta.Models
{
    internal class IdpTokenResponse
    {
        [JsonProperty(PropertyName = "cookieToken")]
        internal string CookieToken { get; set; }
        [JsonProperty(PropertyName = "sessionToken")]
        internal string SessionToken { get; set; }
    }
}