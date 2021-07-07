/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

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
        public StringContent ResponseContent { get; set; }

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
            response.Content = ResponseContent;
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
