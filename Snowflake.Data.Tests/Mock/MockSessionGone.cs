using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

internal sealed class MockSessionGone : MockBaseRestRequester
{
    private const int SESSION_GONE_CODE = 390111;

    public override Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult(Post<T>(request));

    public override T Post<T>(IRestRequest postRequest)
    {
        var sfRequest = (SFRestRequest)postRequest;
        if (sfRequest.jsonBody is LoginRequest)
        {
            object authnResponse = new LoginResponse
            {
                data = new LoginResponseData()
                {
                    token = "session_token",
                    masterToken = "master_token",
                    authResponseSessionInfo = new SessionInfo(),
                    nameValueParameter = new List<NameValueParameter>()
                },
                success = true
            };
            return (T)authnResponse;
        }

        if (sfRequest.jsonBody is QueryRequest)
        {
            object queryExecResponse = new QueryExecResponse
            {
                success = false,
                code = SESSION_GONE_CODE
            };
            return (T)queryExecResponse;
        }

        if (sfRequest.Url.AbsolutePath == "/session")
        {
            object closeResponse = new CloseResponse()
            {
                success = false,
            };
            return (T)closeResponse;
        }

        return default(T);
    }

    public override T Get<T>(IRestRequest request)
    {
        if (typeof(T) == typeof(QueryStatusResponse))
        {
            object queryStatusResponse = new QueryStatusResponse
            {
                success = false,
                code = SESSION_GONE_CODE
            };
            return (T)queryStatusResponse;
        }

        object queryExecResponse = new QueryExecResponse
        {
            success = false,
            code = SESSION_GONE_CODE
        };
        return (T)queryExecResponse;
    }

    public override Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult(Get<T>(request));
}
