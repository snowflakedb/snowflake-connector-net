using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

internal sealed class MockNeverRespondingRestRequester : MockBaseRestRequester
{
    public override async Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
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

        if (sfRequest.jsonBody is QueryCancelRequest)
            return (T)(object)new NullDataResponse { success = true };

        if (sfRequest.jsonBody == null && typeof(T) == typeof(CloseResponse))
            return (T)(object)new CloseResponse { success = true };

        // For query requests: wait until cancelled
        await Task.Delay(TimeSpan.FromMinutes(30), cancellationToken).ConfigureAwait(false);
        return default; // never reached
    }

    public override T Post<T>(IRestRequest request) => Task.Run(async () => await PostAsync<T>(request, CancellationToken.None).ConfigureAwait(false)).Result;

    public override Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult<T>(default);

    public override T Get<T>(IRestRequest request) => default;
}
