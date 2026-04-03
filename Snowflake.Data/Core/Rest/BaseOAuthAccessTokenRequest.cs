using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

namespace Snowflake.Data.Core.Rest
{
    internal abstract class BaseOAuthAccessTokenRequest
    {
        public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

        public string TokenEndpoint { get; set; }

        public string ClientId { get; set; }

        public string ClientSecret { get; set; }

        public string AuthorizationScope { get; set; }

        public TimeSpan HttpTimeout { get; set; } = DefaultTimeout;

        public TimeSpan RestTimeout { get; set; } = DefaultTimeout;

        protected abstract Dictionary<string, string> GetRequestValues();

        public HttpRequestMessage CreateHttpRequest()
        {
            var authorizationHeader = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{ClientId}:{ClientSecret}"));
            var requestMessage = new HttpRequestMessage(HttpMethod.Post, TokenEndpoint);
            requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Basic", authorizationHeader);
            requestMessage.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            requestMessage.Content = GetContent();
            requestMessage.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, HttpTimeout);
            requestMessage.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, RestTimeout);
            return requestMessage;
        }

        private FormUrlEncodedContent GetContent()
        {
            var values = GetRequestValues();
            return BuildFormUrlEncodedContent(values);
        }

        private FormUrlEncodedContent BuildFormUrlEncodedContent(Dictionary<string, string> values)
        {
            var content = new FormUrlEncodedContent(values);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/x-www-form-urlencoded");
            content.Headers.ContentEncoding.Add("utf-8");
            return content;
        }
    }
}
