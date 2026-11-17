using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock;

// Tells whether a session was closed with the blocking or with the asynchronous request.
internal class MockCloseTrackingRestRequester : MockBaseRestRequester
{
    private int _blockingCloseCalls;
    private int _asyncCloseCalls;

    internal int BlockingCloseCalls => Volatile.Read(ref _blockingCloseCalls);

    internal int AsyncCloseCalls => Volatile.Read(ref _asyncCloseCalls);

    public override T Post<T>(IRestRequest postRequest)
    {
        if (IsCloseRequest(postRequest))
            Interlocked.Increment(ref _blockingCloseCalls);
        return Respond<T>(postRequest);
    }

    public override Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
    {
        if (IsCloseRequest(postRequest))
            Interlocked.Increment(ref _asyncCloseCalls);
        return Task.FromResult(Respond<T>(postRequest));
    }

    private static T Respond<T>(IRestRequest request)
    {
        var sfRequest = (SFRestRequest)request;
        if (sfRequest.jsonBody is LoginRequest)
        {
            var authnResponse = new LoginResponse
            {
                data = new LoginResponseData()
                {
                    sessionId = "123456789",
                    token = "session_token",
                    masterToken = "master_token",
                    authResponseSessionInfo = new SessionInfo(),
                    nameValueParameter = new List<NameValueParameter>()
                },
                success = true
            };
            return (T)(object)authnResponse;
        }

        if (IsCloseRequest(request))
            return (T)(object)new CloseResponse { success = true };

        throw new NotImplementedException();
    }

    private static bool IsCloseRequest(IRestRequest request) =>
        ((SFRestRequest)request).Url.Query.StartsWith("?delete=true");
}
