using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock
{
    class MockRestRequesterUnauthorizedOnQuery : IMockRestRequester
    {
        public T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await PostAsync<T>(postRequest, CancellationToken.None).ConfigureAwait(false)).Result;
        }

        public Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)postRequest;
            if (sfRequest.jsonBody is LoginRequest)
            {
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
                return Task.FromResult<T>((T)(object)authnResponse);
            }

            if (sfRequest.jsonBody is QueryRequest)
            {
                var ex = new HttpRequestException("Response status code does not indicate success: 401 (Unauthorized).");
                ex.Data[RestRequester.HttpStatusCodeDataKey] = (int)HttpStatusCode.Unauthorized;
                throw ex;
            }

            if (sfRequest.jsonBody == null && typeof(T) == typeof(CloseResponse))
            {
                return Task.FromResult<T>((T)(object)new CloseResponse { success = true });
            }

            return Task.FromResult<T>((T)(object)null);
        }

        public T Get<T>(IRestRequest request)
        {
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None).ConfigureAwait(false)).Result;
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

        public void setHttpClient(HttpClient httpClient)
        {
        }
    }
}
