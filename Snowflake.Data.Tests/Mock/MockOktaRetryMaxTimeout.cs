/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Newtonsoft.Json;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using System;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{

    class MockOktaRetryMaxTimeout : RestRequester, IMockRestRequester
    {
        internal bool _forceTimeoutForNonLoginRequestsOnly = false;
        internal int _maxRetryCount;
        internal int _maxRetryTimeout;

        public MockOktaRetryMaxTimeout(int maxRetryCount, int maxRetryTimeout) : base(null)
        {
            _maxRetryCount = maxRetryCount;
            _maxRetryTimeout = maxRetryTimeout;
        }

        public void setHttpClient(HttpClient httpClient)
        {
            _HttpClient = httpClient;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage message,
                                                              TimeSpan restTimeout,
                                                              CancellationToken externalCancellationToken,
                                                              string sid = "")
        {
            if (HttpUtil.IsOktaSSORequest(message.RequestUri.Host, message.RequestUri.AbsolutePath))
            {
                var mockContent = new StringContent(JsonConvert.SerializeObject("<form=error}"), Encoding.UTF8, "application/json");
                mockContent.Headers.Add(OktaAuthenticator.RetryCountHeader, _maxRetryCount.ToString());
                mockContent.Headers.Add(OktaAuthenticator.TimeoutElapsedHeader, _maxRetryTimeout.ToString());

                return new HttpResponseMessage
                {
                    StatusCode = System.Net.HttpStatusCode.OK,
                    Content = mockContent
                };
            }

            return await base.SendAsync(message, restTimeout, externalCancellationToken).ConfigureAwait(false);
        }
    }
}
