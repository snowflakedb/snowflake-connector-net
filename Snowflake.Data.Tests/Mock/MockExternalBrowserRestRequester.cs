/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{

    class MockExternalBrowserRestRequester : IMockRestRequester
    {
        public string ProofKey { get; set; }
        public string SSOUrl { get; set; }

        public T Get<T>(IRestRequest request)
        {
            throw new System.NotImplementedException();
        }

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        public T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await (PostAsync<T>(postRequest, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)postRequest;
            if (sfRequest.jsonBody is AuthenticatorRequest)
            {
                if (string.IsNullOrEmpty(SSOUrl))
                {
                    var body = (AuthenticatorRequest)sfRequest.jsonBody;
                    var port = body.Data.BrowserModeRedirectPort;
                    SSOUrl = $"http://localhost:{port}/?token=mockToken";
                }

                // authenticator
                var authnResponse = new AuthenticatorResponse
                {
                    success = true,
                    data = new AuthenticatorResponseData
                    {
                        proofKey = ProofKey,
                        ssoUrl = SSOUrl,
                    }
                };

                return Task.FromResult<T>((T)(object)authnResponse);
            }
            else
            {
                // login
                var loginResponse = new LoginResponse
                {
                    success = true,
                    data = new LoginResponseData
                    {
                        sessionId = "",
                        token = "",
                        masterToken = "",
                        masterValidityInSeconds = 0,
                        authResponseSessionInfo = new SessionInfo
                        {
                            databaseName = "",
                            schemaName = "",
                            roleName = "",
                            warehouseName = "",
                        }
                    }
                };

                return Task.FromResult<T>((T)(object)loginResponse);
            }
        }

        public HttpResponseMessage Get(IRestRequest request)
        {
            throw new System.NotImplementedException();
        }

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        public void setHttpClient(HttpClient httpClient)
        {
            // Nothing to do
        }
    }
}
