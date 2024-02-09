using Newtonsoft.Json;

namespace Snowflake.Data.Core.Authenticator.Okta.Models
{
    internal class IdpTokenRequest
    {
        [JsonProperty(PropertyName = "username")]
        internal string Username { get; set; }

        [JsonProperty(PropertyName = "password")]
        internal string Password { get; set; }
    }
}