/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Newtonsoft.Json;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{

    class MockRetryUntilRestTimeoutRestRequester : RestRequester, IMockRestRequester
    {
        public MockRetryUntilRestTimeoutRestRequester() : base(null)
        {
            // Does nothing
        }

        public void setHttpClient(HttpClient httpClient)
        {
            base._HttpClient = httpClient;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage message,
                                                              TimeSpan restTimeout,
                                                              CancellationToken externalCancellationToken)
        {
            // Override the http timeout and set to 1ms to force all http request to timeout and retry
            message.Properties[BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY] = TimeSpan.FromMilliseconds(1);
            return await (base.SendAsync(message, restTimeout, externalCancellationToken).ConfigureAwait(false));
        }
    }
}

