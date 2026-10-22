using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;

namespace Snowflake.Data.Tests.Mock;

using Snowflake.Data.Core;

internal class MockServiceName : MockBaseRestRequester
{
    public const string INIT_SERVICE_NAME = "init";
    public override Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
    {
        var message = request.ToRequestMessage(HttpMethod.Post);
        var param = new NameValueParameter { name = "SERVICE_NAME" };
        if (!message.Headers.Contains("X-Snowflake-Service"))
        {
            param.value = INIT_SERVICE_NAME;
        }
        else
        {
            IEnumerable<string> headerValues = message.Headers.GetValues("X-Snowflake-Service");
            foreach (string value in headerValues)
            {
                param.value = value + 'a';
            }
        }

        SFRestRequest sfRequest = (SFRestRequest)request;
        if (sfRequest.jsonBody is LoginRequest)
        {
            LoginResponse authnResponse = new LoginResponse
            {
                data = new LoginResponseData()
                {
                    token = "session_token",
                    masterToken = "master_token",
                    authResponseSessionInfo = new SessionInfo(),
                    nameValueParameter = new List<NameValueParameter>() { param }
                },
                success = true
            };

            // login request return success
            return Task.FromResult<T>((T)(object)authnResponse);
        }
        else if (sfRequest.jsonBody is QueryRequest)
        {

            QueryExecResponse queryExecResponse = new QueryExecResponse
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
                    parameters = new List<NameValueParameter> { param }
                }
            };
            return Task.FromResult<T>((T)(object)queryExecResponse);
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
        return Task.FromResult<T>((T)(object)null);
    }
}
