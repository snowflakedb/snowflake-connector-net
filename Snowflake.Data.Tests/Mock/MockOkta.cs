using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{

    class MockOktaRestRequester : IMockRestRequester
    {
        public string TokenUrl { get; set; }
        public string SSOUrl { get; set; }
        public string ResponseContent { get; set; }
        public int MaxRetryCount { get; set; }
        public int MaxRetryTimeout { get; set; }

        public T Get<T>(IRestRequest request)
        {
            throw new System.NotImplementedException();
        }

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        {
            var response = new HttpResponseMessage(System.Net.HttpStatusCode.OK);
            response.Content = new StringContent(ResponseContent);
            response.Content.Headers.Add(OktaAuthenticator.RetryCountHeader, MaxRetryCount.ToString());
            response.Content.Headers.Add(OktaAuthenticator.TimeoutElapsedHeader, MaxRetryTimeout.ToString());
            return Task.FromResult(response);
        }

        public T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await (PostAsync<T>(postRequest, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
        {
            if (postRequest is SFRestRequest)
            {
                if (((SFRestRequest)postRequest).jsonBody is AuthenticatorRequest)
                {
                    // authenticator
                    var authnResponse = new AuthenticatorResponse
                    {
                        success = true,
                        data = new AuthenticatorResponseData
                        {
                            tokenUrl = TokenUrl,
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
            else
            {
                //idp onetime token
                IdpTokenResponse tokenResponse = new IdpTokenResponse
                {
                    CookieToken = "cookie",
                };
                return Task.FromResult<T>((T)(object)tokenResponse);
            }
        }

        public HttpResponseMessage Get(IRestRequest request)
        {
            return Task.Run(async () => await (GetAsync(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public void setHttpClient(HttpClient httpClient)
        {
            // Nothing to do
        }
    }
}
