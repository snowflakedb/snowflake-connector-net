using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock;

using Snowflake.Data.Core;

internal class MockRestSessionExpiredInQueryExec : MockBaseRestRequester
{
    private static readonly int QUERY_IN_EXEC_CODE = 333333;

    private static readonly int SESSION_EXPIRED_CODE = 390112;

    private int getResultCallCount;

    public override Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
    {
        var sfRequest = (SFRestRequest)request;
        if (sfRequest.jsonBody is LoginRequest)
        {
            var authnResponse = new LoginResponse
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

            // login request return success
            return Task.FromResult<T>((T)(object)authnResponse);
        }
        else if (sfRequest.jsonBody is QueryRequest)
        {
            var queryExecResponse = new QueryExecResponse
            {
                success = false,
                code = QUERY_IN_EXEC_CODE
            };
            return Task.FromResult<T>((T)(object)queryExecResponse);

        }
        else if (sfRequest.jsonBody is RenewSessionRequest)
        {
            return Task.FromResult<T>((T)(object)new RenewSessionResponse
            {
                success = true,
                data = new RenewSessionResponseData()
                {
                    sessionToken = "new_session_token"
                }
            });
        }
        else
        {
            return Task.FromResult<T>((T)(object)null);
        }
    }

    public override T Post<T>(IRestRequest postRequest)
    {
        return Task.Run(async () => await (PostAsync<T>(postRequest, CancellationToken.None)).ConfigureAwait(false)).Result;
    }

    public override T Get<T>(IRestRequest request)
    {
        return Task.Run(async () => await (GetAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
    }

    public override Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
    {
        var sfRequest = (SFRestRequest)request;
        if (getResultCallCount == 0)
        {
            getResultCallCount++;
            var queryExecResponse = new QueryExecResponse
            {
                success = false,
                code = QUERY_IN_EXEC_CODE
            };
            return Task.FromResult<T>((T)(object)queryExecResponse);
        }
        else if (getResultCallCount == 1)
        {
            getResultCallCount++;
            var queryExecResponse = new QueryExecResponse
            {
                success = false,
                code = SESSION_EXPIRED_CODE
            };
            return Task.FromResult<T>((T)(object)queryExecResponse);
        }
        else if (getResultCallCount == 2 &&
                 sfRequest.authorizationToken.Equals("Snowflake Token=\"new_session_token\""))
        {
            getResultCallCount++;
            var queryExecResponse = new QueryExecResponse
            {
                success = true,
                data = new QueryExecResponseData
                {
                    rowSet = new string[,] { { "1" } },
                    rowType = new List<ExecResponseRowType>()
                    {
                        new()
                        {
                            name = "colone",
                            type = "FIXED"
                        }
                    },
                    parameters = new List<NameValueParameter>()
                }
            };
            return Task.FromResult<T>((T)(object)queryExecResponse);
        }
        else
        {
            var queryExecResponse = new QueryExecResponse
            {
                success = false,
                code = 1
            };
            return Task.FromResult<T>((T)(object)queryExecResponse);
        }
    }
}
