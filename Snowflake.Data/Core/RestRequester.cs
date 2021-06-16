/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using Newtonsoft.Json;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    /// <summary>
    /// The RestRequester is responsible to send out a rest request and receive response
    /// </summary>
    internal interface IRestRequester
    {
        Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken);

        T Post<T>(IRestRequest postRequest);

        Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken);

        T Get<T>(IRestRequest request);

        Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken);

        HttpResponseMessage Get(IRestRequest request);
    }

    internal interface IMockRestRequester : IRestRequester
    {
        void setHttpClient(HttpClient httpClient);
    }

    internal class RestRequester : IRestRequester
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<RestRequester>();

        protected HttpClient _HttpClient;

        public RestRequester(HttpClient httpClient)
        {
            _HttpClient = httpClient;
        }

        public T Post<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (PostAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public async Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            var response = await SendAsync(HttpMethod.Post, request, cancellationToken).ConfigureAwait(false);
            var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(json);
        }

        public T Get<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (GetAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public async Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = await GetAsync(request, cancellationToken).ConfigureAwait(false);
            var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(json);
        }
        
        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        {
            return SendAsync(HttpMethod.Get, request, cancellationToken);
        }

        public HttpResponseMessage Get(IRestRequest request)
        {
            HttpRequestMessage message = request.ToRequestMessage(HttpMethod.Get);

            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (GetAsync(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        private async Task<HttpResponseMessage> SendAsync(HttpMethod method,
                                                          IRestRequest request,
                                                          CancellationToken externalCancellationToken)
        {
            HttpRequestMessage message = request.ToRequestMessage(method);

            return await SendAsync(message, request.GetRestTimeout(), externalCancellationToken).ConfigureAwait(false);
        }

        protected virtual async Task<HttpResponseMessage> SendAsync(HttpRequestMessage message,
                                                              TimeSpan restTimeout, 
                                                              CancellationToken externalCancellationToken)
        {
            // merge multiple cancellation token
            CancellationTokenSource restRequestTimeout = new CancellationTokenSource(restTimeout);
            CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken,
                restRequestTimeout.Token);

            try
            {
                var response = await _HttpClient
                    .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, linkedCts.Token)
                    .ConfigureAwait(false);
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
