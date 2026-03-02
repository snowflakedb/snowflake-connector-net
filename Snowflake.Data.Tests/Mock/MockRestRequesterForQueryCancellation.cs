using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock
{
    class MockRestRequesterForQueryCancellation : IMockRestRequester
    {
        private static readonly string SessionToken = "mock_session_token";

        internal bool CancelRequestSent { get; private set; }
        internal string CancelledQueryId { get; private set; }

        internal Action OnGetQueryStatusResponse { get; set; }

        public Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)request;
            if (sfRequest.jsonBody is LoginRequest)
            {
                return Task.FromResult<T>((T)(object)new LoginResponse
                {
                    data = new LoginResponseData()
                    {
                        token = SessionToken,
                        masterToken = "master_token",
                        authResponseSessionInfo = new SessionInfo(),
                        nameValueParameter = new List<NameValueParameter>()
                    },
                    success = true
                });
            }
            else if (sfRequest.jsonBody is QueryRequest queryRequest &&
                     queryRequest.sqlText != null &&
                     queryRequest.sqlText.Contains("SYSTEM$CANCEL_QUERY"))
            {
                CancelRequestSent = true;
                var startIdx = queryRequest.sqlText.IndexOf("SYSTEM$CANCEL_QUERY('", StringComparison.Ordinal);
                if (startIdx >= 0)
                {
                    startIdx += "SYSTEM$CANCEL_QUERY('".Length;
                    var endIdx = queryRequest.sqlText.IndexOf("'", startIdx, StringComparison.Ordinal);
                    if (endIdx > startIdx)
                    {
                        CancelledQueryId = queryRequest.sqlText.Substring(startIdx, endIdx - startIdx);
                    }
                }

                return Task.FromResult<T>((T)(object)new QueryExecResponse
                {
                    success = true,
                    data = new QueryExecResponseData
                    {
                        rowSet = new string[,] { { "query cancelled" } },
                        rowType = new List<ExecResponseRowType>
                        {
                            new ExecResponseRowType { name = "result", type = "TEXT" }
                        },
                        parameters = new List<NameValueParameter>()
                    }
                });
            }
            else if (sfRequest.jsonBody == null)
            {
                if (typeof(T) == typeof(CloseResponse))
                {
                    return Task.FromResult<T>((T)(object)new CloseResponse { success = true });
                }
                return Task.FromResult<T>((T)(object)new NullDataResponse { success = true });
            }
            return Task.FromResult<T>((T)(object)null);
        }

        public T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await PostAsync<T>(postRequest, CancellationToken.None)).Result;
        }

        public T Get<T>(IRestRequest request)
        {
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None)).Result;
        }

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)request;

            if (sfRequest._isStatusRequest)
            {
                var response = new QueryStatusResponse
                {
                    success = true,
                    data = new QueryStatusData
                    {
                        queries = new List<QueryStatusDataQueries>
                        {
                            new QueryStatusDataQueries
                            {
                                status = "RUNNING"
                            }
                        }
                    }
                };

                OnGetQueryStatusResponse?.Invoke();

                return Task.FromResult<T>((T)(object)response);
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

        public void setHttpClient(HttpClient httpClient)
        {
        }
    }
}
