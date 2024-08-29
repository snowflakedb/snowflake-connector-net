/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core;
using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{

    class MockInfiniteTimeout : RestRequester, IMockRestRequester
    {
        public MockInfiniteTimeout() : base(null)
        {
            // Does nothing
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
            message.Properties[BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY] = Timeout.InfiniteTimeSpan;
            return await (base.SendAsync(message, restTimeout, externalCancellationToken).ConfigureAwait(false));
        }
    }
}

