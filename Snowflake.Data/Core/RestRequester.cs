/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections;
using System.Linq;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    /// <summary>
    /// The RestRequester is responsible to send out a rest request and receive response
    /// No retry needed here since retry is made in HttpClient.RetryHandler (HttpUtil.cs)
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
            using (var response = await SendAsync(HttpMethod.Post, request, cancellationToken).ConfigureAwait(false))
            {
                var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JsonConvert.DeserializeObject<T>(json, JsonUtils.JsonSettings);
            }
        }

        public T Get<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (GetAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public async Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            using (HttpResponseMessage response = await GetAsync(request, cancellationToken).ConfigureAwait(false))
            {
                var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JsonConvert.DeserializeObject<T>(json, JsonUtils.JsonSettings);
            }
        }

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        {
            return SendAsync(HttpMethod.Get, request, cancellationToken);
        }

        public HttpResponseMessage Get(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (GetAsync(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        private async Task<HttpResponseMessage> SendAsync(HttpMethod method,
                                                          IRestRequest request,
                                                          CancellationToken externalCancellationToken)
        {
            using (HttpRequestMessage message = request.ToRequestMessage(method))
            {
                return await SendAsync(message, request.GetRestTimeout(), externalCancellationToken, request.getSid()).ConfigureAwait(false);
            }
        }

        protected virtual async Task<HttpResponseMessage> SendAsync(HttpRequestMessage message,
                                                              TimeSpan restTimeout,
                                                              CancellationToken externalCancellationToken,
                                                              string sid="")
        {
            // merge multiple cancellation token
            using (CancellationTokenSource restRequestTimeout = new CancellationTokenSource(restTimeout))
            {
                using (CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken,
                restRequestTimeout.Token))
                {
                    HttpResponseMessage response = null;
                    try
                    {
                        message.Headers.Add("threadid", Thread.CurrentThread.ManagedThreadId.ToString());

                        logger.Debug($"### Request: {sid}\n{message.Method}\n{message.RequestUri}\nHTTP/{message.Version}\n{ObfuscateHttpHeaders(message.Headers)}\n{message.Content?.ReadAsStringAsync().Result}");

                        response = await _HttpClient
                            .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, linkedCts.Token)
                            .ConfigureAwait(false);
                        if (!response.IsSuccessStatusCode)
                        {
                            logger.Error($"Failed Response: {sid} {message.Method} {message.RequestUri} StatusCode: {(int)response.StatusCode}, ReasonPhrase: '{response.ReasonPhrase}'");
                        }
                        else
                        {
                            logger.Debug($"Succeeded Response: {sid} {message.Method} {message.RequestUri}");
                        }

                        if (response.RequestMessage.RequestUri.ToString().Contains("snowflakecomputing.com"))
                        {
                            logger.Debug(
                                $"### Response: {sid} {response.RequestMessage.RequestUri}\nHTTP/{response.Version} {(int)response.StatusCode} {response.StatusCode}\n{ObfuscateHttpHeaders(response.Headers)}\n{response.Content?.ReadAsStringAsync().Result}");
                        }
                        else
                        {
                            logger.Debug(
                                $"### Response: {sid} {response.RequestMessage.RequestUri}\nHTTP/{response.Version} {(int)response.StatusCode} {response.StatusCode}\n{ObfuscateHttpHeaders(response.Headers)}");
                        }

                        response.EnsureSuccessStatusCode();
                        return response;
                    }
                    catch (Exception e)
                    {
                        // Disposing of the response if not null now that we don't need it anymore
                        response?.Dispose();
                        if (restRequestTimeout.IsCancellationRequested)
                        {
                            throw new SnowflakeDbException(e, SFError.REQUEST_TIMEOUT);
                        }
                        throw;
                    }
                }
            }
        }

        private static string ObfuscateHttpHeaders(HttpHeaders httpRequestHeaders)
        {
            var s = httpRequestHeaders.ToString();
            var lines = s.Split('\n');
            var safe = new StringBuilder();
            foreach(var l in lines)
            {
                if (l.StartsWith("x-ms-encryption-key"))
                {
                    var pair = l.Split(':');
                    safe.AppendLine($"{pair[0]}: ***");
                }
                else
                    safe.AppendLine(l);
            }

            var headers = safe.ToString();
            return headers;
        }
    }
}
