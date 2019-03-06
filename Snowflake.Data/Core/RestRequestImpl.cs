/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using System.Net.Http;
using System;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Log;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    public class RestRequestImpl : IRestRequest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<RestRequestImpl>();

        private static readonly RestRequestImpl instance = new RestRequestImpl();

        private static MediaTypeHeaderValue applicationJson = new MediaTypeHeaderValue("applicaion/json");

        private static MediaTypeWithQualityHeaderValue applicationSnowflake = new MediaTypeWithQualityHeaderValue("application/snowflake");

        private const string SF_AUTHORIZATION_HEADER = "Authorization";

        private const string SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm";

        private const string SSE_C_KEY = "x-amz-server-side-encryption-customer-key";

        private const string SSE_C_AES = "AES256";

        private RestRequestImpl()
        {
        }
        
        static internal RestRequestImpl Instance
        {
            get { return instance; }
        }

        public T Post<T>(SFRestRequest postRequest)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await PostAsync<T>(postRequest, CancellationToken.None)).Result;
        }

        public async Task<T> PostAsync<T>(SFRestRequest postRequest, CancellationToken cancellationToken)
        {
            var req = ToRequestMessage(HttpMethod.Post, postRequest);

            var response = await SendAsync(req, postRequest.sfRestRequestTimeout, cancellationToken);
            var json = await response.Content.ReadAsStringAsync();
            logger.Debug($"Post response: {json}");
            return JsonConvert.DeserializeObject<T>(json);
        }

        public T Get<T>(SFRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None)).Result;
        }

        public async Task<T> GetAsync<T>(SFRestRequest request, CancellationToken cancellationToken)
        {
            var req = ToRequestMessage(HttpMethod.Get, request);

            var response = await SendAsync(req, request.sfRestRequestTimeout, cancellationToken);
            var json = await response.Content.ReadAsStringAsync();
            logger.Debug($"Get response: {json}");
            return JsonConvert.DeserializeObject<T>(json);
        }

        private HttpRequestMessage ToRequestMessage(HttpMethod method, SFRestRequest request)
        {
            var msg = new HttpRequestMessage(method, request.uri);
            if (method != HttpMethod.Get && request.jsonBody != null)
            {
                var json = JsonConvert.SerializeObject(request.jsonBody);
                //TODO: Check if we should use other encodings...
                msg.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }

            msg.Headers.Add(SF_AUTHORIZATION_HEADER, request.authorizationToken);
            msg.Headers.Accept.Add(applicationSnowflake);
            
            msg.Properties["TIMEOUT_PER_HTTP_REQUEST"] = request.httpRequestTimeout;

            logger.Debug($"Http method: {method.ToString()}, http request message: {msg.ToString()}");

            return msg;
        }
        
        public Task<HttpResponseMessage> GetAsync(S3DownloadRequest getRequest, CancellationToken cancellationToken)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, getRequest.uri);

            if (getRequest.chunkHeaders != null)
            {
                foreach(var item in getRequest.chunkHeaders)
                {
                    message.Headers.Add(item.Key, item.Value);
                }
            }
            else
            {
                message.Headers.Add(SSE_C_ALGORITHM, SSE_C_AES);
                message.Headers.Add(SSE_C_KEY, getRequest.qrmk);
            }
            message.Properties["TIMEOUT_PER_HTTP_REQUEST"] = getRequest.httpRequestTimeout;

            logger.Debug($"S3 Download request message {message.ToString()}");

            return SendAsync(message, getRequest.timeout, cancellationToken);
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
