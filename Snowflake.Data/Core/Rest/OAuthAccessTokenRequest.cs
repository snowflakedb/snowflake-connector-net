using System.Collections.Generic;

namespace Snowflake.Data.Core.Rest
{
    internal class OAuthAccessTokenRequest : BaseOAuthAccessTokenRequest
    {
        public string GrantType { get; set; }

        public string AuthorizationCode { get; set; }

        public string CodeVerifier { get; set; }

        public string RedirectUri { get; set; }

        protected override Dictionary<string, string> GetRequestValues()
        {
            var requestValues = new Dictionary<string, string>
            {
                { "grant_type", GrantType },
                { "scope", AuthorizationScope },
            };
            if (!string.IsNullOrEmpty(AuthorizationCode))
                requestValues.Add("code", AuthorizationCode);
            if (!string.IsNullOrEmpty(CodeVerifier))
                requestValues.Add("code_verifier", CodeVerifier);
            if (!string.IsNullOrEmpty(RedirectUri))
                requestValues.Add("redirect_uri", RedirectUri);
            return requestValues;
        }
    }
}
