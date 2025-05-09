using System.Text;
using System.Web;
using Snowflake.Data.Core.Authenticator.Browser;

namespace Snowflake.Data.Core.Rest
{
    internal class OAuthAuthorizationCodeRequest
    {
        public string AuthorizationEndpoint { get; set; }
        public string AuthorizationScope { get; set; }
        public string ClientId { get; set; }
        public string RedirectUri { get; set; }
        public string CodeChallenge { get; set; }
        public string State { get; set; }

        public Url GetUrl() =>
            new Url(CreateUrl(true), CreateUrl(false));

        private string CreateUrl(bool includeSecrets)
        {
            var urlBuilder = new StringBuilder();
            urlBuilder.Append(AuthorizationEndpoint);
            var clientId = includeSecrets ? HttpUtility.UrlEncode(ClientId) : "****";
            urlBuilder.Append("?client_id=").Append(clientId);
            urlBuilder.Append("&response_type=").Append(HttpUtility.UrlEncode("code"));
            urlBuilder.Append("&redirect_uri=").Append(HttpUtility.UrlEncode(RedirectUri));
            urlBuilder.Append("&scope=").Append(HttpUtility.UrlEncode(AuthorizationScope));
            urlBuilder.Append("&code_challenge=").Append(HttpUtility.UrlEncode(CodeChallenge));
            urlBuilder.Append("&code_challenge_method=").Append(HttpUtility.UrlEncode("S256"));
            var state = includeSecrets ? HttpUtility.UrlEncode(State) : "****";
            urlBuilder.Append("&state=").Append(state);
            return urlBuilder.ToString();
        }
    }
}
