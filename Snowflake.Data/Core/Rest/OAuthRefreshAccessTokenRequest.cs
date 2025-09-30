using System.Collections.Generic;

namespace Snowflake.Data.Core.Rest
{
    internal class OAuthRefreshAccessTokenRequest : BaseOAuthAccessTokenRequest
    {
        public string RefreshToken { get; set; }

        protected override Dictionary<string, string> GetRequestValues()
        {
            var values = new Dictionary<string, string>
            {
                { "grant_type", "refresh_token" },
                { "refresh_token", RefreshToken }
            };
            if (!string.IsNullOrEmpty(AuthorizationScope))
            {
                values.Add("scope", AuthorizationScope);
            }
            return values;
        }
    }
}
