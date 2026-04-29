using Snowflake.Data.Core;
using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{
    internal sealed class MockRetryUntilRestTimeoutRestRequester : RestRequester, IMockRestRequester
    {
        private readonly bool _forceTimeoutAlsoForLoginRequests;

        public MockRetryUntilRestTimeoutRestRequester(bool forceTimeoutAlsoForLoginRequests = true) : base(null)
        {
            _forceTimeoutAlsoForLoginRequests = forceTimeoutAlsoForLoginRequests;
        }

        public void setHttpClient(HttpClient httpClient)
        {
            base._HttpClient = httpClient;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage message,
                                                              TimeSpan restTimeout,
                                                              CancellationToken externalCancellationToken,
                                                              string sid = "")
        {
            if (_forceTimeoutAlsoForLoginRequests || !message.RequestUri.AbsolutePath.Equals(RestPath.SF_LOGIN_PATH))
            {
                // Override the http timeout and set to 1ms to force all http request to timeout and retry
                message.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromTicks(0));
            }

            return await (base.SendAsync(message, restTimeout, externalCancellationToken).ConfigureAwait(false));
        }
    }
}

