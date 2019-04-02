/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Text;

namespace Snowflake.Data.Core
{
    /**
     * The RestRequester is responsible to send out a rest request and receive response
     */
    public interface IRestRequester
    {
        Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken);

        T Post<T>(IRestRequest postRequest);

        T Get<T>(IRestRequest request);

        Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken);

        Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken);
    }

    public interface IRestRequest
    {
        HttpRequestMessage ToRequestMessage(HttpMethod method);
        TimeSpan RestTimeout();
    }

    public class S3DownloadRequest : IRestRequest
    {
        private const string SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm";

        private const string SSE_C_KEY = "x-amz-server-side-encryption-customer-key";

        private const string SSE_C_AES = "AES256";

        internal Uri uri{ get; set; }

        internal string qrmk { get; set; }

        // request timeout in millis
        internal TimeSpan timeout { get; set; }

        // timeout for each http request 
        internal TimeSpan httpRequestTimeout { get; set; }

        internal Dictionary<string, string> chunkHeaders { get; set; }

        public HttpRequestMessage ToRequestMessage(HttpMethod method)
        {
            HttpRequestMessage message = new HttpRequestMessage(method, uri);
            if (chunkHeaders != null)
            {
                foreach (var item in chunkHeaders)
                {
                    message.Headers.Add(item.Key, item.Value);
                }
            } else
            {
                message.Headers.Add(SSE_C_ALGORITHM, SSE_C_AES);
                message.Headers.Add(SSE_C_KEY, qrmk);
            }

            message.Properties["TIMEOUT_PER_HTTP_REQUEST"] = httpRequestTimeout;

            return message;
        }

        public TimeSpan RestTimeout()
        {
            return timeout;
        }
    }

    public class SFRestRequest : IRestRequest
    {
        private static MediaTypeWithQualityHeaderValue applicationSnowflake = new MediaTypeWithQualityHeaderValue("application/snowflake");

        private const string SF_AUTHORIZATION_HEADER = "Authorization";

        public SFRestRequest()
        {
            sfRestRequestTimeout = Timeout.InfiniteTimeSpan;

            // default each http request timeout to 16 seconds
            httpRequestTimeout = TimeSpan.FromSeconds(16); 
        }

        internal Uri uri { get; set; }

        internal Object jsonBody { get; set;  }

        internal String authorizationToken { get; set; }

        internal String serviceName { get; set; }
        
        // timeout for the whole rest request in millis (adding up all http retry)
        internal TimeSpan sfRestRequestTimeout { get; set; }
        
        // timeout for each http request 
        internal TimeSpan httpRequestTimeout { get; set; }

        public override string ToString()
        {
            return String.Format("SFRestRequest {{url: {0}, request body: {1} }}", uri.ToString(), 
                jsonBody.ToString());
        }

        public HttpRequestMessage ToRequestMessage(HttpMethod method)
        {
            var message = new HttpRequestMessage(method, uri);
            if (method != HttpMethod.Get && jsonBody != null)
            {
                var json = JsonConvert.SerializeObject(jsonBody);
                //TODO: Check if we should use other encodings...
                message.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }

            message.Headers.Add(SF_AUTHORIZATION_HEADER, authorizationToken);
            message.Headers.Accept.Add(applicationSnowflake);

            message.Properties["TIMEOUT_PER_HTTP_REQUEST"] = httpRequestTimeout;
            
            return message;
        }

        public TimeSpan RestTimeout()
        {
            return sfRestRequestTimeout;
        }
    }

    class AuthnRequest
    {
        [JsonProperty(PropertyName = "data")]
        internal AuthnRequestData data { get; set; }

        public override string ToString()
        {
            return String.Format("AuthRequest {{data: {0} }}", data.ToString());
        }
    }

    class AuthnRequestData
    {
        [JsonProperty(PropertyName = "CLIENT_APP_ID")]
        internal String clientAppId { get; set; }

        [JsonProperty(PropertyName = "CLIENT_APP_VERSION")]
        internal String clientAppVersion { get; set; }

        [JsonProperty(PropertyName = "ACCOUNT_NAME", NullValueHandling = NullValueHandling.Ignore)]
        internal String accountName { get; set; }

        [JsonProperty(PropertyName = "LOGIN_NAME")]
        internal String loginName { get; set; }

        [JsonProperty(PropertyName = "PASSWORD")]
        internal String password { get; set; }

        [JsonProperty(PropertyName = "CLIENT_ENVIRONMENT")]
        internal AuthnRequestClientEnv clientEnv { get; set; }

        public override string ToString()
        {
            return String.Format("AuthRequestData {{ClientAppVersion: {0} AccountName: {1}, loginName: {2}, ClientEnv: {3} }}", 
                clientAppVersion, accountName, loginName, clientEnv.ToString());
        }
    }

    class AuthnRequestClientEnv
    {
        [JsonProperty(PropertyName = "APPLICATION")]
        internal String application { get; set; }

        [JsonProperty(PropertyName = "OS_VERSION")]
        internal String osVersion { get; set; }

        [JsonProperty(PropertyName = "NET_RUNTIME")]
        internal String netRuntime { get; set; }

        public override string ToString()
        {
            return String.Format("{{ APPLICATION: {0}, OS_VERSION: {1}, NET_RUNTIME: {2} }}", 
                application, osVersion, netRuntime);
        }
    }

    class QueryRequest
    {
        [JsonProperty(PropertyName = "sqlText")]
        internal string sqlText { get; set; }

        [JsonProperty(PropertyName = "describeOnly")]
        internal bool describeOnly { get; set; }

        [JsonProperty(PropertyName = "bindings")]
        internal Dictionary<string, BindingDTO> parameterBindings { get; set; }
    }

    class QueryCancelRequest
    {
        [JsonProperty(PropertyName = "requestId")]
        internal string requestId { get; set; }
    }

    class RenewSessionRequest
    {
        [JsonProperty(PropertyName = "oldSessionToken")]
        internal string oldSessionToken { get; set; }

        [JsonProperty(PropertyName = "requestType")]
        internal string requestType { get; set; }
    }
}
