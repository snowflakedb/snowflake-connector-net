using System;
using System.Net.Http;

namespace Snowflake.Data.Core.Rest
{
    internal class RestRequestWrapper: BaseRestRequest, IRestRequest
    {
        private readonly HttpRequestMessage _httpRequestMessage;
        private readonly TimeSpan _restTimeout;

        public RestRequestWrapper(HttpRequestMessage httpRequestMessage, TimeSpan restTimeout)
        {
            _httpRequestMessage = httpRequestMessage;
            _restTimeout = restTimeout;
        }

        public HttpRequestMessage ToRequestMessage(HttpMethod method)
        {
            return _httpRequestMessage;
        }

        public TimeSpan GetRestTimeout()
        {
            return _restTimeout;
        }
    }
}
