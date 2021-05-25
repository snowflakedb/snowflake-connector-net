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
using System.Net;
using System.Collections.Generic;

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

    internal class RestRequester : IRestRequester
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<RestRequester>();

        private static Dictionary<string, IRestRequester> RestRequestersByProxy = 
            new Dictionary<string, IRestRequester>();

        private static readonly object requesterPoolLock = new object();

        // The proxy to use making the requests
        internal IWebProxy Proxy;

        private RestRequester(IWebProxy proxy)
        {
            Proxy = proxy;
        }

        /// <summary>
        /// Get the RestRequester associated with the given proxy information or create a new one
        /// if none exist in the pool already.
        /// </summary>
        /// <param name="proxyHost">The proxy host.</param>
        /// <param name="proxyPort">The proxy port.</param>
        /// <param name="proxyUser">The proxy username or null if none.</param>
        /// <param name="proxyPassword">The proxy password or null if none.</param>
        /// <param name="noProxyList">The list of urls to by-pass the proxy.</param>
        /// <returns>The RestRequester for this proxy.</returns>
        /// <exception cref="System.FormatException">The port is not a valid int</exception>
        /// <exception cref="System.OverflowException">The port value is too large</exception>
        internal static IRestRequester GetRestRequester(
            string proxyHost,
            string proxyPort,
            string proxyUser,
            string proxyPassword,
            string noProxyList)
        {
            string key = string.Join(";", new string[]{ proxyHost, proxyPort, proxyUser, proxyPassword, noProxyList });
            lock(requesterPoolLock)
            {
                if (!RestRequestersByProxy.TryGetValue(key, out IRestRequester requester))
                {
                    WebProxy webProxy = null;
                    if (null != proxyHost)
                    {
                        // New proxy needed
                        webProxy = new WebProxy(proxyHost, int.Parse(proxyPort));

                        // Add credential if provided
                        if (!String.IsNullOrEmpty(proxyUser))
                        {
                            ICredentials credentials = new NetworkCredential(proxyUser, proxyPassword);
                            webProxy.Credentials = credentials;
                        }

                        // Add bypasslist if provided
                        if (!String.IsNullOrEmpty(noProxyList))
                        {
                            string[] bypassList = noProxyList.Split(
                                new char[] { '|' }, 
                                StringSplitOptions.RemoveEmptyEntries);
                            // Convert simplified syntax to standard regular expression syntax
                            string entry = null;
                            for (int i = 0; i < bypassList.Length; i++)
                            {
                                // Get the original entry
                                entry = bypassList[i].Trim();
                                // . -> [.] because . means any char 
                                entry = entry.Replace(".", "[.]");
                                // * -> .*  because * is a quantifier and need a char or group to apply to
                                entry = entry.Replace("*", ".*");
                            
                                // Replace with the valid entry syntax
                                bypassList[i] = entry;

                            }
                            webProxy.BypassList = bypassList;
                        }
                    }
                    requester = new RestRequester(webProxy);
                    RestRequestersByProxy.Add(key, requester);
                }

                return requester;
            }
        }
        public T Post<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await PostAsync<T>(request, CancellationToken.None)).Result;
        }

        public async Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            var req = request.ToRequestMessage(HttpMethod.Post);

            var response = await SendAsync(req, request.GetRestTimeout(), cancellationToken).ConfigureAwait(false);
            var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            return JsonConvert.DeserializeObject<T>(json);
        }

        public T Get<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None)).Result;
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

        public HttpResponseMessage Get(IRestRequest request)
        {
            HttpRequestMessage message = request.ToRequestMessage(HttpMethod.Get);

            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await GetAsync(request, CancellationToken.None)).Result;
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
                //logger.Debug("Execute request with proxy " + ((null != Proxy) ? Proxy.ToString() : "no proxy"));
                HttpClient httpClient = HttpUtil.getHttpClient(Proxy);
                var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, linkedCts.Token).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                return response;
            }
            catch(Exception e)
            {
                if (restRequestTimeout.IsCancellationRequested)
                {
                    // Timeout or cancellation
                    throw new SnowflakeDbException(e, SFError.REQUEST_TIMEOUT);
                }
                else
                {
                    //rethrow
                    throw;
                }
            }
        }
    }
    
}
