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

    class MockRetryUntilRestTimeoutRestRequester : IRestRequester
    {
        public T Get<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None)).Result;
        }

        public HttpResponseMessage Get(IRestRequest request)
        {
            HttpRequestMessage message = request.ToRequestMessage(HttpMethod.Get);

            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await GetAsync(request, CancellationToken.None)).Result;
        }

        public async Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = await GetAsync(request, cancellationToken).ConfigureAwait(false);
            var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(json);
        }

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        {
            HttpRequestMessage message = request.ToRequestMessage(HttpMethod.Get);

            return SendAsync(message, request.GetRestTimeout(), cancellationToken);
        }

        public T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await PostAsync<T>(postRequest, CancellationToken.None)).Result;
        }

        public async Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
        {
            var req = postRequest.ToRequestMessage(HttpMethod.Post);
            var response = await SendAsync(req, postRequest.GetRestTimeout(), cancellationToken).ConfigureAwait(false);
            var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(json);
        }

        private async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                                                          TimeSpan timeoutPerRestRequest,
                                                          CancellationToken externalCancellationToken)
        {
            // merge multiple cancellation token
            CancellationTokenSource restRequestTimeout = new CancellationTokenSource(timeoutPerRestRequest);
            CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken,
                restRequestTimeout.Token);

            try
            {
                // Http timeout of 1ms to force retries
                request.Properties[BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY] = TimeSpan.FromMilliseconds(1);
                var response = await HttpUtil.getHttpClient(false).SendAsync(request, HttpCompletionOption.ResponseHeadersRead, linkedCts.Token).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                return response;
            }
            catch (Exception e)
            {
                throw restRequestTimeout.IsCancellationRequested ? new SnowflakeDbException(SFError.REQUEST_TIMEOUT) : e;
            }

        }
    }
}
