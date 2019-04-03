/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using System.Net.Http;
using System;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    public class RestRequesterImpl : IRestRequester
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<RestRequesterImpl>();

        private static readonly RestRequesterImpl instance = new RestRequesterImpl();

        private RestRequesterImpl()
        {
        }
        
        static internal RestRequesterImpl Instance
        {
            get { return instance; }
        }

        public T Post<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await PostAsync<T>(request, CancellationToken.None)).Result;
        }

        public async Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            var req = request.ToRequestMessage(HttpMethod.Post);

            var response = await SendAsync(req, request.RestTimeout(), cancellationToken);
            var json = await response.Content.ReadAsStringAsync();
            logger.Debug($"Post response: {json}");
            return JsonConvert.DeserializeObject<T>(json);
        }

        public T Get<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None)).Result;
        }

        public async Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = await GetAsync(request, cancellationToken);
            var json = await response.Content.ReadAsStringAsync();
            logger.Debug($"Get response: {json}");
            return JsonConvert.DeserializeObject<T>(json);
        }
        
        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken)
        {
            HttpRequestMessage message = request.ToRequestMessage(HttpMethod.Get);
            logger.Debug($"Http method: {message.ToString()}, http request message: {message.ToString()}");

            return SendAsync(message, request.RestTimeout(), cancellationToken);
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
                var response = await HttpUtil.getHttpClient().SendAsync(request, linkedCts.Token);
                response.EnsureSuccessStatusCode();

                return response;
            }
            catch(Exception e)
            {
                throw restRequestTimeout.IsCancellationRequested ? new SnowflakeDbException(SFError.REQUEST_TIMEOUT) : e;
            }
        }
    }
}
