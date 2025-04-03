using System.Text;
using System.Web;

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

        public string GetUrl()
        {
            var urlBuilder = new StringBuilder();
            urlBuilder.Append(AuthorizationEndpoint);
            urlBuilder.Append("?client_id=").Append(HttpUtility.UrlEncode(ClientId));
            urlBuilder.Append("&response_type=").Append(HttpUtility.UrlEncode("code"));
            urlBuilder.Append("&redirect_uri=").Append(HttpUtility.UrlEncode(RedirectUri));
            urlBuilder.Append("&scope=").Append(HttpUtility.UrlEncode(AuthorizationScope));
            urlBuilder.Append("&code_challenge=").Append(HttpUtility.UrlEncode(CodeChallenge));
            urlBuilder.Append("&code_challenge_method=").Append(HttpUtility.UrlEncode("S256"));
            urlBuilder.Append("&state=").Append(HttpUtility.UrlEncode(State));
            return urlBuilder.ToString();
        }
    }
}
