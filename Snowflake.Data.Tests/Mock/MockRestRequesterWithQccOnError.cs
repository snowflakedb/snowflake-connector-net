using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Http;

namespace Snowflake.Data.Tests.Mock
{
    using Snowflake.Data.Core;

    class MockRestRequesterWithQccOnError : IMockRestRequester
    {
        internal const int FAILED_QUERY_CODE = 999;
        internal const string FAILED_QUERY_MESSAGE = "mock query failure";

        internal static readonly ResponseQueryContext MockQueryContext = new ResponseQueryContext
        {
            Entries = new List<ResponseQueryContextElement>
            {
                new ResponseQueryContextElement(new QueryContextElement(42, 1000L, 1, "mock_context"))
            }
        };

        public Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)request;
            if (sfRequest.jsonBody is LoginRequest)
            {
                var authnResponse = new LoginResponse
                {
                    data = new LoginResponseData()
                    {
                        token = "mock_session_token",
                        masterToken = "mock_master_token",
                        authResponseSessionInfo = new SessionInfo(),
                        nameValueParameter = new List<NameValueParameter>()
                    },
                    success = true
                };
                return Task.FromResult<T>((T)(object)authnResponse);
            }
            else if (sfRequest.jsonBody is QueryRequest)
            {
                var queryExecResponse = new QueryExecResponse
                {
                    success = false,
                    code = FAILED_QUERY_CODE,
                    message = FAILED_QUERY_MESSAGE,
                    data = new QueryExecResponseData
                    {
                        sqlState = "42000",
                        queryId = "mock-query-id",
                        QueryContext = MockQueryContext,
                        parameters = new List<NameValueParameter>()
                    }
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
            }
            else
            {
                return Task.FromResult<T>(default(T));
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
            return Task.FromResult<T>(default(T));
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
