using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock
{
    class MockLoginStoringRestRequester : IMockRestRequester
    {
        internal List<LoginRequest> LoginRequests { get; } = new();

        public T Get<T>(IRestRequest request)
        {
            return Task.Run(async () => await (GetAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult<T>((T)(object)null);
        }

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult<HttpResponseMessage>(null);
        }

        public HttpResponseMessage Get(IRestRequest request)
        {
            return null;
        }

        public T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await (PostAsync<T>(postRequest, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)postRequest;
            if (sfRequest.jsonBody is LoginRequest)
            {
                LoginRequests.Add((LoginRequest)sfRequest.jsonBody);
                LoginResponse authnResponse = new LoginResponse
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
            throw new NotImplementedException();
        }

        public void setHttpClient(HttpClient httpClient)
        {
            // Nothing to do
        }
    }
}
