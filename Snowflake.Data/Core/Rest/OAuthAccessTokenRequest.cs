using System.Collections.Generic;

namespace Snowflake.Data.Core.Rest
{
    internal class OAuthAccessTokenRequest: BaseOAuthAccessTokenRequest
    {
        public string AuthorizationCode { get; set; }

        public string CodeVerifier { get; set; }

        public string RedirectUri { get; set; }

        protected override Dictionary<string, string> GetRequestValues() =>
            new Dictionary<string, string>
            {
                { "grant_type",  "authorization_code" },
                { "code", AuthorizationCode },
                { "code_verifier", CodeVerifier },
                { "scope",  AuthorizationScope },
                { "redirect_uri", RedirectUri }
            };
    }
}
