using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

/// <summary>
/// Mock that returns a "query in progress" response with a queryId on the first Post,
/// then throws OperationCanceledException on Get (polling), simulating a timeout
/// during the result polling loop.
/// </summary>
internal sealed class MockTimeoutDuringPollingRestRequester : IMockRestRequester
{
    internal const string MockQueryId = "mock-polling-timeout-query-id";

    public Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
    {
        return Task.FromResult(Post<T>(request));
    }

    public T Post<T>(IRestRequest request)
    {
        var sfRequest = (SFRestRequest)request;
        if (sfRequest.jsonBody is LoginRequest)
        {
            return (T)(object)new LoginResponse
            {
                data = new LoginResponseData
                {
                    token = "mock_token",
                    masterToken = "mock_master_token",
                    authResponseSessionInfo = new SessionInfo(),
                    nameValueParameter = new List<NameValueParameter>()
                },
                success = true
            };
        }

        if (sfRequest.jsonBody is QueryRequest)
        {
            return (T)(object)new QueryExecResponse
            {
                success = true,
                code = 333333,
                data = new QueryExecResponseData
                {
                    queryId = MockQueryId,
                    getResultUrl = $"/queries/{MockQueryId}/result",
                    rowType = [],
                    parameters = []
                }
            };
        }

        if (sfRequest.jsonBody is QueryCancelRequest)
            return (T)(object)new NullDataResponse { success = true };

        if (sfRequest.jsonBody == null && typeof(T) == typeof(CloseResponse))
            return (T)(object)new CloseResponse { success = true };

        return (T)(object)new NullDataResponse { success = true };
    }

    public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
    {
        throw new OperationCanceledException(cancellationToken);
    }

    public T Get<T>(IRestRequest request)
    {
        throw new OperationCanceledException();
    }

    public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        => Task.FromResult<HttpResponseMessage>(null);

    public HttpResponseMessage Get(IRestRequest request) => null;

    public void setHttpClient(HttpClient httpClient) { }
}
