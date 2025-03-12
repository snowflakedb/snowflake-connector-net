using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

namespace Snowflake.Data.Core.Rest
{
    public class OAuthAccessTokenRequest
    {
        public string TokenEndpoint { get; set; }

        public string AuthorizationCode { get; set; }

        public string AuthorizationScope { get; set; }

        public string ClientId { get; set; }

        public string ClientSecret { get; set; }

        public string CodeVerifier { get; set; }

        public string RedirectUri { get; set; }

        public TimeSpan HttpTimeout { get; set; } = TimeSpan.FromSeconds(60);

        public TimeSpan RestTimeout { get; set; } = TimeSpan.FromSeconds(60);

        public HttpRequestMessage CreateHttpRequest()
        {
            var authorizationHeader = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{ClientId}:{ClientSecret}"));
            var requestMessage = new HttpRequestMessage(HttpMethod.Post, TokenEndpoint);
            requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Basic", authorizationHeader);
            requestMessage.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            requestMessage.Content = GetContent();
            requestMessage.Properties.Add("TIMEOUT_PER_HTTP_REQUEST", HttpTimeout);
            requestMessage.Properties.Add("TIMEOUT_PER_REST_REQUEST", RestTimeout);
            return requestMessage;
        }

        private FormUrlEncodedContent GetContent()
        {
            var values = new Dictionary<string, string>
            {
                { "grant_type",  "authorization_code" },
                { "code", AuthorizationCode },
                { "code_verifier", CodeVerifier },
                { "scope",  AuthorizationScope },
                { "redirect_uri", RedirectUri }
            };
            var content = new FormUrlEncodedContent(values);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/x-www-form-urlencoded");
            content.Headers.ContentEncoding.Add("utf-8");
            return content;
        }
    }
}
