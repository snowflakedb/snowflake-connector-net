using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;

namespace Snowflake.Data.Tests.Mock
{
    using Snowflake.Data.Core;

    class MockRestSessionExpiredInQueryExec : IRestRequest
    {
        static private readonly int QUERY_IN_EXEC_CODE = 333333;

        static private readonly int SESSION_EXPIRED_CODE = 390112;

        private int getResultCallCount = 0;

        public MockRestSessionExpiredInQueryExec() { }

        public Task<T> PostAsync<T>(SFRestRequest postRequest, CancellationToken cancellationToken)
        {
            if (postRequest.jsonBody is AuthnRequest)
            {
                AuthnResponse authnResponse = new AuthnResponse
                {
                    data = new AuthnResponseData()
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
            else if (postRequest.jsonBody is QueryRequest)
            {
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = false,
                    code = QUERY_IN_EXEC_CODE
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
                
            }
            else if (postRequest.jsonBody is RenewSessionRequest)
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

        public T Post<T>(SFRestRequest postRequest)
        {
            return Task.Run(async () => await PostAsync<T>(postRequest, CancellationToken.None)).Result;
        }

        public T Get<T>(SFRestRequest request)
        {
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None)).Result;
        }

        public Task<T> GetAsync<T>(SFRestRequest request, CancellationToken cancellationToken)
        {
            if (getResultCallCount == 0)
            {
                getResultCallCount++;
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = false,
                    code = QUERY_IN_EXEC_CODE
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
            }
            else if (getResultCallCount == 1)
            {
                getResultCallCount++;
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = false,
                    code = SESSION_EXPIRED_CODE
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
            }
            else if (getResultCallCount == 2 && 
                request.authorizationToken.Equals("Snowflake Token=\"new_session_token\""))
            {
                getResultCallCount++;
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = true,
                    data = new QueryExecResponseData
                    {
                        rowSet = new string[,] { { "1" } },
                        rowType = new List<ExecResponseRowType>()
                            {
                                new ExecResponseRowType
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
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = false,
                    code = 1
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
            }
        }

        public Task<HttpResponseMessage> GetAsync(S3DownloadRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult<HttpResponseMessage>(null);
        }
    }
}

