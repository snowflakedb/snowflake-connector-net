using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock
{
    internal sealed class MockRestSessionNoLongerExists : IMockRestRequester
    {
        private const int SESSION_NO_LONGER_EXISTS_CODE = 390111;

        public Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult(Post<T>(request));

        public T Post<T>(IRestRequest postRequest)
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
                    code = SESSION_NO_LONGER_EXISTS_CODE
                };
                return (T)queryExecResponse;
            }

            return default(T);
        }

        public T Get<T>(IRestRequest request)
        {
            object queryExecResponse = new QueryExecResponse
            {
                success = false,
                code = SESSION_NO_LONGER_EXISTS_CODE
            };
            return (T)queryExecResponse;
        }

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult(Get<T>(request));

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult<HttpResponseMessage>(null);

        public HttpResponseMessage Get(IRestRequest request) => null;

        public void setHttpClient(HttpClient httpClient)
        {
            // Nothing to do
        }
    }
}
