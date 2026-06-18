using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Http;

namespace Snowflake.Data.Tests.Mock
{
    using Snowflake.Data.Core;

    class MockRestSessionExpired : IMockRestRequester
    {
        static private readonly String EXPIRED_SESSION_TOKEN = "session_expired_token";

        static internal readonly String NEW_SESSION_TOKEN = "new_session_token";

        static private readonly String TOKEN_FMT = "Snowflake Token=\"{0}\"";

        static internal readonly String THROW_ERROR_TOKEN = "throw_error_token";

        static internal readonly int SESSION_EXPIRED_CODE = 390112;

        public string FirstTimeRequestID;

        public string SecondTimeRequestID;

        public MockRestSessionExpired() { }

        public Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)request;
            if (sfRequest.jsonBody is LoginRequest)
            {
                LoginResponse authnResponse = new LoginResponse
                {
                    data = new LoginResponseData()
                    {
                        token = EXPIRED_SESSION_TOKEN,
                        masterToken = "master_token",
                        authResponseSessionInfo = new SessionInfo(),
                        nameValueParameter = new List<NameValueParameter>()
                    },
                    success = true
                };

                // login request return success
                return Task.FromResult<T>((T)(object)authnResponse);
            }
            else if (sfRequest.jsonBody is QueryRequest)
            {
                if (sfRequest.authorizationToken.Equals(String.Format(TOKEN_FMT, EXPIRED_SESSION_TOKEN)))
                {
                    FirstTimeRequestID = ExtractRequestID(sfRequest.Url.Query);
                    QueryExecResponse queryExecResponse = new QueryExecResponse
                    {
                        success = false,
                        code = SESSION_EXPIRED_CODE
                    };
                    return Task.FromResult<T>((T)(object)queryExecResponse);
                }
                else if (sfRequest.authorizationToken.Equals(String.Format(TOKEN_FMT, NEW_SESSION_TOKEN)))
                {
                    SecondTimeRequestID = ExtractRequestID(sfRequest.Url.Query);
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
            else if (sfRequest.jsonBody is RenewSessionRequest renewSessionRequest)
            {
                if (renewSessionRequest.oldSessionToken == THROW_ERROR_TOKEN)
                {
                    throw new Exception("Error while renewing session");
                }
                return Task.FromResult<T>((T)(object)new RenewSessionResponse
                {
                    success = true,
                    data = new RenewSessionResponseData()
                    {
                        sessionToken = NEW_SESSION_TOKEN,
                        masterToken = "new_master_token"
                    }
                });
            }
            else if (sfRequest.jsonBody == null)
            {
                if (typeof(T) == typeof(CloseResponse))
                {
                    return Task.FromResult<T>((T)(object)new CloseResponse
                    {
                        success = true
                    });
                }
                return Task.FromResult<T>((T)(object)new NullDataResponse
                {
                    success = true
                });
            }
            else
            {
                return Task.FromResult<T>((T)(object)null);
            }
        }

        public T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await (PostAsync<T>(postRequest, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public T Get<T>(IRestRequest request)
        {
            return Task.Run(async () => await (GetAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)request;
            if (sfRequest.Url.ToString().Contains("retryId"))
            {
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = false,
                    code = SESSION_EXPIRED_CODE
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
            }
            if (sfRequest.authorizationToken.Equals(String.Format(TOKEN_FMT, EXPIRED_SESSION_TOKEN)))
            {
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = false,
                    code = SESSION_EXPIRED_CODE
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
            }
            if (sfRequest.authorizationToken.Equals(String.Format(TOKEN_FMT, NEW_SESSION_TOKEN)))
            {
                QueryExecResponse queryExecResponse = new QueryExecResponse
                {
                    success = true,
                    data = new QueryExecResponseData
                    {
                        rowSet = new string[,] { { "abc" } },
                        rowType = new List<ExecResponseRowType>()
                            {
                                new ExecResponseRowType
                                {
                                    name = "colOne",
                                    type = SFDataType.TEXT.ToString()
                                }
                            },
                        parameters = new List<NameValueParameter>()
                    }
                };
                return Task.FromResult<T>((T)(object)queryExecResponse);
            }
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

        private string ExtractRequestID(string queries)
        {
            int start = queries.IndexOf("requestId=");
            start += 10;
            return queries.Substring(start, 36);
        }

        public void setHttpClient(HttpClient httpClient)
        {
            // Nothing to do
        }
    }
}
