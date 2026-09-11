using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

internal sealed class MockRestRequesterForDescribeOnly : IMockRestRequester
{
    internal bool? CapturedDescribeOnly { get; private set; }

    public Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
    {
        var sfRequest = (SFRestRequest)request;
        if (sfRequest.jsonBody is LoginRequest)
        {
            var response = new LoginResponse
            {
                data = new LoginResponseData
                {
                    token = "session_token",
                    masterToken = "master_token",
                    authResponseSessionInfo = new SessionInfo(),
                    nameValueParameter = new List<NameValueParameter>()
                },
                success = true
            };
            return Task.FromResult((T)(object)response);
        }

        if (sfRequest.jsonBody is QueryRequest queryRequest)
        {
            if (CapturedDescribeOnly != null)
                throw new InvalidOperationException("Multiple queries executed. Asserts are meaningless.");

            CapturedDescribeOnly = queryRequest.describeOnly;

            var queryResponse = new QueryExecResponse
            {
                success = true,
                data = new QueryExecResponseData
                {
                    rowSet = new[,] { { "1" } },
                    rowType =
                    [
                        new()
                        {
                            name = "col1",
                            type = "FIXED"
                        }
                    ],
                    parameters = new List<NameValueParameter>()
                }
            };
            return Task.FromResult((T)(object)queryResponse);
        }

        return Task.FromResult(default(T));
    }

    public T Post<T>(IRestRequest request) => PostAsync<T>(request, CancellationToken.None).Result;

    public T Get<T>(IRestRequest request) => default;

    public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult(default(T));

    public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult<HttpResponseMessage>(null);

    public HttpResponseMessage Get(IRestRequest request) => null;

    public void setHttpClient(HttpClient httpClient) { }
}
