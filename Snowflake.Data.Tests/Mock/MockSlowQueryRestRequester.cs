using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

internal sealed class MockSlowQueryRestRequester : IMockRestRequester
{
    internal const string MockQueryId = "mock-slow-query-id-001";

    private readonly TimeSpan _queryDelay;

    internal MockSlowQueryRestRequester(TimeSpan queryDelay)
    {
        _queryDelay = queryDelay;
    }

    public async Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
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
            await Task.Delay(_queryDelay, cancellationToken).ConfigureAwait(false);
            return (T)(object)new QueryExecResponse
            {
                success = true,
                data = new QueryExecResponseData
                {
                    queryId = MockQueryId,
                    rowSet = new[,] { { "1" } },
                    rowType = [new ExecResponseRowType { name = "result", type = "TEXT" }],
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

    public T Post<T>(IRestRequest request) => Task.Run(async () => await PostAsync<T>(request, CancellationToken.None).ConfigureAwait(false)).Result;

    public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult<T>(default);

    public T Get<T>(IRestRequest request) => default;

    public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult<HttpResponseMessage>(null);

    public HttpResponseMessage Get(IRestRequest request) => null;

    public void setHttpClient(HttpClient httpClient)
    {
    }
}
