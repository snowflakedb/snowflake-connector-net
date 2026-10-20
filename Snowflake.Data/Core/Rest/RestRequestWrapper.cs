using System;
using System.Net.Http;
using Snowflake.Data.Core.Extensions;

namespace Snowflake.Data.Core.Rest;

internal class RestRequestWrapper : BaseRestRequest, IRestRequest
{
    private readonly HttpRequestMessage _httpRequestMessage;

    public RestRequestWrapper(HttpRequestMessage httpRequestMessage)
    {
        _httpRequestMessage = httpRequestMessage;
        if (httpRequestMessage.TryGetOption(REST_REQUEST_TIMEOUT_KEY, out var timeout) && timeout is TimeSpan restTimeout)
            RestTimeout = restTimeout;

        if (httpRequestMessage.TryGetOption(HTTP_REQUEST_TIMEOUT_KEY, out timeout) && timeout is TimeSpan httpTimeout)
            HttpTimeout = httpTimeout;
    }

    public HttpRequestMessage ToRequestMessage(HttpMethod method) => _httpRequestMessage;
}
