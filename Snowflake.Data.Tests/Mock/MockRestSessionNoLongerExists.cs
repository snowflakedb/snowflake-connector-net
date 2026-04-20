using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;

namespace Snowflake.Data.Tests.Mock
{
    using Snowflake.Data.Core;

    class MockRestSessionNoLongerExists : IMockRestRequester
    {
        internal const int SESSION_NO_LONGER_EXISTS_CODE = 390111;

        public Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
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
                        nameValueParameter = new List<NameValueParameter>()
                    },
                    success = true
                };
                return Task.FromResult<T>((T)(object)authnResponse);
            }
            else if (sfRequest.jsonBody is QueryRequest)
            {
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = false,
                    code = SESSION_NO_LONGER_EXISTS_CODE
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
            }
            else
            {
                return Task.FromResult<T>((T)(object)null);
            }
        }

        public T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await PostAsync<T>(postRequest, CancellationToken.None).ConfigureAwait(false)).Result;
        }

        public T Get<T>(IRestRequest request)
        {
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None).ConfigureAwait(false)).Result;
        }

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            QueryExecResponse queryExecResponse = new QueryExecResponse
            {
                success = false,
                code = SESSION_NO_LONGER_EXISTS_CODE
            };
            return Task.FromResult<T>((T)(object)queryExecResponse);
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
            // Nothing to do
        }
    }
}
