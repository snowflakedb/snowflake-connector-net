/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using System.Net.Http;
using System;
using System.Net.Http.Headers;
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
            HttpContent httpContent = new StringContent(json);
            CancellationTokenSource cancellationTokenSource = 
                new CancellationTokenSource(postRequest.sfRestRequestTimeout);

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, postRequest.uri);
            message.Properties["TIMEOUT_PER_HTTP_REQUEST"] = postRequest.httpRequestTimeout;

            message.Content = httpContent;
            message.Content.Headers.ContentType = applicationJson;
            message.Headers.Add(SF_AUTHORIZATION_HEADER, postRequest.authorizationToken);
            message.Headers.Accept.Add(applicationSnowflake);

            var responseContent = sendRequest(message, cancellationTokenSource.Token).Content;

            var jsonString = responseContent.ReadAsStringAsync();
            jsonString.Wait();

            return JObject.Parse(jsonString.Result);
        }

        public HttpResponseMessage get(S3DownloadRequest getRequest)
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

            CancellationTokenSource cancellationTokenSource = 
                new CancellationTokenSource(getRequest.timeout);

            return sendRequest(message, cancellationTokenSource.Token);
        }

        public JObject get(SFRestRequest getRequest)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, getRequest.uri);
            message.Headers.Add(SF_AUTHORIZATION_HEADER, getRequest.authorizationToken);
            message.Headers.Accept.Add(applicationSnowflake);
            message.Properties["TIMEOUT_PER_HTTP_REQUEST"] = getRequest.httpRequestTimeout;

            CancellationTokenSource cancellationTokenSource = 
                new CancellationTokenSource(getRequest.sfRestRequestTimeout);

            var responseContent = sendRequest(message, cancellationTokenSource.Token).Content;

            var jsonString = responseContent.ReadAsStringAsync();
            jsonString.Wait();

            return JObject.Parse(jsonString.Result);
        }

        private HttpResponseMessage sendRequest(HttpRequestMessage requestMessage, CancellationToken cancellationToken)
        {
            try
            {
                var response = HttpUtil.getHttpClient().SendAsync(requestMessage, cancellationToken)
                    .Result.EnsureSuccessStatusCode();

                return response;
            }
            catch(AggregateException e)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new SnowflakeDbException(SFError.REQUEST_TIMEOUT);
                }
                else
                {
                    throw e;
                }
            }
        }
    }

}
