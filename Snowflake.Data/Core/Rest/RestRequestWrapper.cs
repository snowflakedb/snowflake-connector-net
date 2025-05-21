using System;
using System.Net.Http;

namespace Snowflake.Data.Core.Rest
{
    internal class RestRequestWrapper : BaseRestRequest, IRestRequest
    {
        private readonly HttpRequestMessage _httpRequestMessage;

        public RestRequestWrapper(HttpRequestMessage httpRequestMessage)
        {
            _httpRequestMessage = httpRequestMessage;
        }

        public HttpRequestMessage ToRequestMessage(HttpMethod method)
        {
            return _httpRequestMessage;
        }

        public TimeSpan GetRestTimeout() =>
            (TimeSpan)_httpRequestMessage.Properties[REST_REQUEST_TIMEOUT_KEY];
    }
}
