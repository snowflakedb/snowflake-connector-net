using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

/// <summary>
/// Mock that returns a successful PutGetExecResponse with a queryId, but then the
/// caller (SFFileTransferAgent) will fail because the data is incomplete.
/// Used to verify that ExecuteSqlWithPutGet exception handling works correctly.
/// </summary>
internal sealed class MockPutGetOceRestRequester : IMockRestRequester
{
    internal const string MockQueryId = "mock-putget-query-id";
    private readonly bool _throwOceOnQuery;

    /// <param name="throwOceOnQuery">
    /// When true, throws OperationCanceledException on query Post (simulating HTTP timeout).
    /// When false, returns a successful PutGetExecResponse (file transfer agent will fail).
    /// </param>
    internal MockPutGetOceRestRequester(bool throwOceOnQuery = true)
    {
        _throwOceOnQuery = throwOceOnQuery;
    }

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
            if (_throwOceOnQuery)
            {
                var detail = SnowflakeDbException.WithQueryId(
                    new OperationCanceledException("simulated timeout"),
                    SFError.REQUEST_TIMEOUT,
                    MockQueryId,
                    "30");
                throw new OperationCanceledException(detail.Message, detail);
            }

            if (typeof(T) == typeof(PutGetExecResponse))
            {
                return (T)(object)new PutGetExecResponse
                {
                    success = true,
                    data = new PutGetResponseData
                    {
                        queryId = MockQueryId,
                        command = "UPLOAD",
                        parameters = []
                    }
                };
            }

            return (T)(object)new QueryExecResponse
            {
                success = true,
                data = new QueryExecResponseData
                {
                    queryId = MockQueryId,
                    rowType = [],
                    parameters = [],
                    rowSet = new[,] { { "1" } }
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
        => Task.FromResult<T>(default);

    public T Get<T>(IRestRequest request) => default;

    public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        => Task.FromResult<HttpResponseMessage>(null);

    public HttpResponseMessage Get(IRestRequest request) => null;

    public void setHttpClient(HttpClient httpClient) { }
}
