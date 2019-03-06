/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Http;

namespace Snowflake.Data.Tests.Mock
{
    using Snowflake.Data.Core;

    class MockRestSessionExpired : IRestRequest
    {
        static private readonly String EXPIRED_SESSION_TOKEN="session_expired_token";

        static private readonly String TOKEN_FMT = "Snowflake Token=\"{0}\"";

        static private readonly int SESSION_EXPIRED_CODE = 390112;

        public MockRestSessionExpired() { }

        public Task<T> PostAsync<T>(SFRestRequest postRequest, CancellationToken cancellationToken)
        {
            if (postRequest.jsonBody is AuthnRequest)
            {
                AuthnResponse authnResponse = new AuthnResponse
                {
                    data = new AuthnResponseData()
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
            else if (postRequest.jsonBody is QueryRequest)
            {
                if (postRequest.authorizationToken.Equals(String.Format(TOKEN_FMT, EXPIRED_SESSION_TOKEN)))
                {
                    QueryExecResponse queryExecResponse = new QueryExecResponse
                    {
                        success = false,
                        code = SESSION_EXPIRED_CODE
                    };
                    return Task.FromResult<T>((T)(object)queryExecResponse);
                }
                else if (postRequest.authorizationToken.Equals(String.Format(TOKEN_FMT, "new_session_token")))
                {
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
            else if (postRequest.jsonBody is RenewSessionRequest)
            {
                return Task.FromResult<T>((T)(object)new RenewSessionResponse
                {
                    success = true,
                    data = new RenewSessionResponseData()
                    {
                        sessionToken = "new_session_token",
                        masterToken = "new_master_token"
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
            return Task.FromResult<T>((T)(object)null);
        }

        public Task<HttpResponseMessage> GetAsync(S3DownloadRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult<HttpResponseMessage>(null);
        }
    }
}
