using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

internal class MockLoginMFATokenCacheRestRequester : MockBaseRestRequester
{
    internal Queue<LoginRequest> LoginRequests { get; } = new();

    internal Queue<LoginResponseData> LoginResponses { get; } = new();

    public override T Get<T>(IRestRequest request)
    {
        return Task.Run(async () => await (GetAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
    }

    public override T Post<T>(IRestRequest postRequest)
    {
        return Task.Run(async () => await (PostAsync<T>(postRequest, CancellationToken.None)).ConfigureAwait(false)).Result;
    }

    public override Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
    {
        SFRestRequest sfRequest = (SFRestRequest)postRequest;
        if (sfRequest.jsonBody is LoginRequest)
        {
            LoginRequests.Enqueue((LoginRequest)sfRequest.jsonBody);
            var responseData = LoginResponses?.Any() != true ? new LoginResponseData()
            {
                token = "session_token",
                masterToken = "master_token",
                authResponseSessionInfo = new SessionInfo(),
                nameValueParameter = new List<NameValueParameter>()
            } : LoginResponses.Dequeue();
            var authnResponse = new LoginResponse
            {
                data = responseData,
                success = true
            };

            // login request return success
            return Task.FromResult<T>((T)(object)authnResponse);
        }
        else if (sfRequest.jsonBody is CloseResponse)
        {
            var authnResponse = new CloseResponse()
            {
                success = true
            };

            // login request return success
            return Task.FromResult<T>((T)(object)authnResponse);
        }
        throw new NotImplementedException();
    }
}
