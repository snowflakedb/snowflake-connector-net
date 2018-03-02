/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using System.Net.Http;
using System;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Common.Logging;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    public class RestRequestImpl : IRestRequest
    {
        private static ILog logger = LogManager.GetLogger<RestRequestImpl>();

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

        public JObject post(SFRestRequest postRequest)
        {
            var json = JsonConvert.SerializeObject(postRequest.jsonBody);
            HttpContent httpContent = new StringContent(json, Encoding.UTF8, "application/json");

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, postRequest.uri);
            message.Properties["TIMEOUT_PER_HTTP_REQUEST"] = postRequest.httpRequestTimeout;

            message.Content = httpContent;
            message.Content.Headers.ContentType = applicationJson;
            message.Headers.Add(SF_AUTHORIZATION_HEADER, postRequest.authorizationToken);
            message.Headers.Accept.Add(applicationSnowflake);

            var responseContent = sendRequest(message, postRequest.sfRestRequestTimeout).Content;

            var jsonString = responseContent.ReadAsStringAsync();
            jsonString.Wait();

            return JObject.Parse(jsonString.Result);
        }

        public T Post<T>(SFRestRequest postRequest)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await PostAsync<T>(postRequest)).Result;
        }

        public async Task<T> PostAsync<T>(SFRestRequest postRequest)
        {
            var req = ToRequestMessage(HttpMethod.Post, postRequest);

            var response = await SendAsync(req, postRequest.sfRestRequestTimeout);
            var json = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<T>(json);
        }

        public T Get<T>(SFRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await GetAsync<T>(request)).Result;
        }

        public async Task<T> GetAsync<T>(SFRestRequest request)
        {
            var req = ToRequestMessage(HttpMethod.Get, request);

            var response = await SendAsync(req, request.sfRestRequestTimeout);
            var json = await response.Content.ReadAsStringAsync();
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

            return msg;
        }
        
        public Task<HttpResponseMessage> GetAsync(S3DownloadRequest getRequest)
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

            return SendAsync(message, getRequest.timeout);
        }
        
        private async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, TimeSpan timeout)
        {
            var response = await HttpUtil.initHttpClient(timeout).SendAsync(request);
            response.EnsureSuccessStatusCode();

            return response;
        }

        private HttpResponseMessage sendRequest(HttpRequestMessage requestMessage, TimeSpan timeout)
        {
            var response = HttpUtil.initHttpClient(timeout).SendAsync(requestMessage)
                .Result.EnsureSuccessStatusCode();

            return response;
        }
    }

}
