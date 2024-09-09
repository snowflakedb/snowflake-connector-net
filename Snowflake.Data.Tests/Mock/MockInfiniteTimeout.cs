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
        HttpClient mockHttpClient;

        public MockInfiniteTimeout(HttpClient mockHttpClient = null) : base(null)
        {
            this.mockHttpClient = mockHttpClient;
        }

        public void setHttpClient(HttpClient httpClient)
        {
            if (mockHttpClient != null)
            {
                base._HttpClient = mockHttpClient;
            }
            else
            {
                base._HttpClient = httpClient;
            }
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage message,
                                                              TimeSpan restTimeout,
                                                              CancellationToken externalCancellationToken,
                                                              string sid = "")
        {
            // Disable warning as this is the way to be compliant with netstandard2.0
            // API reference: https://learn.microsoft.com/en-us/dotnet/api/system.net.http.httprequestmessage?view=netstandard-2.0
#pragma warning disable CS0618 // Type or member is obsolete
            message.Properties[BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY] = Timeout.InfiniteTimeSpan;
#pragma warning restore CS0618 // Type or member is obsolete
            return await (base.SendAsync(message, restTimeout, externalCancellationToken).ConfigureAwait(false));
        }
    }
}

