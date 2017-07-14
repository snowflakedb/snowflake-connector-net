using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Snowflake.Data.Core
{
    public class RestRequestImpl : IRestRequest
    {
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

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, postRequest.uri);

            message.Content = httpContent;
            message.Content.Headers.ContentType = applicationJson;
            message.Headers.Add(SF_AUTHORIZATION_HEADER, postRequest.authorizationToken);
            message.Headers.Accept.Add(applicationSnowflake);

            var responseContent = sendRequest(message).Content;

            var jsonString = responseContent.ReadAsStringAsync();
            jsonString.Wait();

            return JObject.Parse(jsonString.Result);
        }

        public HttpResponseMessage get(S3DownloadRequest getRequest)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, getRequest.uri);
            message.Headers.Add(SSE_C_ALGORITHM, SSE_C_AES);
            message.Headers.Add(SSE_C_KEY, getRequest.qrmk);

            return sendRequest(message);
        }

        public JObject get(SFRestRequest getRequest)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, getRequest.uri);
            message.Headers.Add(SF_AUTHORIZATION_HEADER, getRequest.authorizationToken);
            message.Headers.Accept.Add(applicationSnowflake);

            var responseContent = sendRequest(message).Content;

            var jsonString = responseContent.ReadAsStringAsync();
            jsonString.Wait();

            return JObject.Parse(jsonString.Result);
        }

        private HttpResponseMessage sendRequest(HttpRequestMessage requestMessage)
        {
            // TODO: wrap up retry logic
            var response = HttpUtil.getHttpClient().SendAsync(requestMessage).Result.EnsureSuccessStatusCode();

            return response;
        }
    }
}
