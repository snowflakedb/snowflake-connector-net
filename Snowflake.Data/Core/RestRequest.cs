/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System.Net.Http;
using System.Net.Http.Headers;
using System;
using System.Text;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Snowflake.Data.Core
{
    internal interface IRestRequest
    {
        HttpRequestMessage ToRequestMessage(HttpMethod method);
        TimeSpan GetRestTimeout();
    }

    /// <summary>
    /// A base rest request implementation with timeout defined
    /// </summary>
    internal abstract class BaseRestRequest : IRestRequest
    {
        internal static string HTTP_REQUEST_TIMEOUT_KEY = "TIMEOUT_PER_HTTP_REQUEST";

        internal static string REST_REQUEST_TIMEOUT_KEY = "TIMEOUT_PER_REST_REQUEST";

        // The default Rest timeout. Set to 120 seconds. 
        public static int DEFAULT_REST_RETRY_SECONDS_TIMEOUT = 120;

        internal Uri Url { get; set; }

        /// <summary>
        /// Timeout of the overall rest request
        /// </summary>
        internal TimeSpan RestTimeout { get; set; }

        /// <summary>
        /// Timeout for every single HTTP request
        /// </summary>
        internal TimeSpan HttpTimeout { get; set; }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            throw new NotImplementedException();
        }

        protected HttpRequestMessage newMessage(HttpMethod method, Uri url)
        {
            HttpRequestMessage message = new HttpRequestMessage(method, url);
            message.Properties[HTTP_REQUEST_TIMEOUT_KEY] = HttpTimeout;
            message.Properties[REST_REQUEST_TIMEOUT_KEY] = RestTimeout;
            return message;
        }

        TimeSpan IRestRequest.GetRestTimeout()
        {
            return RestTimeout;
        }
    }

    internal class S3DownloadRequest : BaseRestRequest, IRestRequest
    {
        private const string SSE_C_ALGORITHM = "x-amz-server-side-encryption-customer-algorithm";

        private const string SSE_C_KEY = "x-amz-server-side-encryption-customer-key";

        private const string SSE_C_AES = "AES256";

        internal string qrmk { get; set; }

        internal Dictionary<string, string> chunkHeaders { get; set; }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            HttpRequestMessage message = newMessage(method, Url);
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

            return message;
        }

    }

    internal class SFRestRequest : BaseRestRequest, IRestRequest
    {
        private static MediaTypeWithQualityHeaderValue applicationSnowflake = new MediaTypeWithQualityHeaderValue("application/snowflake");

        private const string SF_AUTHORIZATION_HEADER = "Authorization";
        private const string SF_SERVICE_NAME_HEADER = "X-Snowflake-Service";

        internal SFRestRequest() : base()
        {
            RestTimeout = TimeSpan.FromSeconds(DEFAULT_REST_RETRY_SECONDS_TIMEOUT);

            // default each http request timeout to 16 seconds
            HttpTimeout = TimeSpan.FromSeconds(16);
        }

        internal Object jsonBody { get; set;  }

        internal String authorizationToken { get; set; }

        internal String serviceName { get; set; }
        
        public override string ToString()
        {
            return String.Format("SFRestRequest {{url: {0}, request body: {1} }}", Url.ToString(), 
                jsonBody.ToString());
        }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            var message = newMessage(method, Url);
            if (method != HttpMethod.Get && jsonBody != null)
            {
                var json = JsonConvert.SerializeObject(jsonBody);
                //TODO: Check if we should use other encodings...
                message.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }

            message.Headers.Add(SF_AUTHORIZATION_HEADER, authorizationToken);
            if (serviceName != null)
            {
                message.Headers.Add(SF_SERVICE_NAME_HEADER, serviceName);
            }

            // add quote otherwise it would be reported as error format
            string osInfo = "(" + SFEnvironment.ClientEnv.osVersion + ")";

            message.Headers.Accept.Add(applicationSnowflake);
            message.Headers.UserAgent.Add(new ProductInfoHeaderValue(SFEnvironment.DriverName, SFEnvironment.DriverVersion));
            message.Headers.UserAgent.Add(new ProductInfoHeaderValue(osInfo));
            message.Headers.UserAgent.Add(new ProductInfoHeaderValue(
                SFEnvironment.ClientEnv.netRuntime,
                SFEnvironment.ClientEnv.netVersion));

            return message;
        }
    }

    class AuthenticatorRequest
    {
        [JsonProperty(PropertyName = "data")]
        internal AuthenticatorRequestData Data { get; set; }

        public override string ToString()
        {
            return String.Format("AuthenticatorRequest {{data: {0} }}", Data.ToString());
        }
    }

    class AuthenticatorRequestData
    {
        [JsonProperty(PropertyName = "ACCOUNT_NAME", NullValueHandling = NullValueHandling.Ignore)]
        internal String AccountName { get; set; }

        [JsonProperty(PropertyName = "AUTHENTICATOR")]
        internal String Authenticator { get; set; }

        [JsonProperty(PropertyName = "BROWSER_MODE_REDIRECT_PORT", NullValueHandling = NullValueHandling.Ignore)]
        internal String BrowserModeRedirectPort { get; set; }

        public override string ToString()
        {
            return String.Format("AuthenticatorRequestData {{ACCOUNT_NANM: {0} }}",
                AccountName.ToString());
        }
    }

    class LoginRequest
    {
        [JsonProperty(PropertyName = "data")]
        internal LoginRequestData data { get; set; }

        public override string ToString()
        {
            return String.Format("LoginRequest {{data: {0} }}", data.ToString());
        }
    }

    class LoginRequestData
    {
        [JsonProperty(PropertyName = "CLIENT_APP_ID")]
        internal String clientAppId { get; set; }

        [JsonProperty(PropertyName = "CLIENT_APP_VERSION")]
        internal String clientAppVersion { get; set; }

        [JsonProperty(PropertyName = "ACCOUNT_NAME", NullValueHandling = NullValueHandling.Ignore)]
        internal String accountName { get; set; }

        [JsonProperty(PropertyName = "LOGIN_NAME", NullValueHandling = NullValueHandling.Ignore)]
        internal String loginName { get; set; }

        [JsonProperty(PropertyName = "PASSWORD", NullValueHandling = NullValueHandling.Ignore)]
        internal String password { get; set; }

        [JsonProperty(PropertyName = "AUTHENTICATOR", NullValueHandling = NullValueHandling.Ignore)]
        internal String Authenticator { get; set; }

        [JsonProperty(PropertyName = "CLIENT_ENVIRONMENT")]
        internal LoginRequestClientEnv clientEnv { get; set; }

        [JsonProperty(PropertyName = "RAW_SAML_RESPONSE", NullValueHandling = NullValueHandling.Ignore)]
        internal String RawSamlResponse { get; set; }

        [JsonProperty(PropertyName = "TOKEN", NullValueHandling = NullValueHandling.Ignore)]
        internal string Token { get; set; }

        [JsonProperty(PropertyName = "PROOF_KEY", NullValueHandling = NullValueHandling.Ignore)]
        internal string ProofKey { get; set; }

        [JsonProperty(PropertyName = "SESSION_PARAMETERS", NullValueHandling = NullValueHandling.Ignore)]
        internal Dictionary<SFSessionParameter, Object> SessionParameters { get; set; }

        public override string ToString()
        {
            return String.Format("LoginRequestData {{ClientAppVersion: {0},\n AccountName: {1},\n loginName: {2},\n ClientEnv: {3},\n authenticator: {4} }}", 
                clientAppVersion, accountName, loginName, clientEnv.ToString(), Authenticator);
        }
    }

    class LoginRequestClientEnv
    {
        [JsonProperty(PropertyName = "APPLICATION")]
        internal String application { get; set; }

        [JsonProperty(PropertyName = "OS_VERSION")]
        internal String osVersion { get; set; }

        [JsonProperty(PropertyName = "NET_RUNTIME")]
        internal String netRuntime { get; set; }

        [JsonProperty(PropertyName = "NET_VERSION")]
        internal string netVersion { get; set; }

        [JsonProperty(PropertyName = "INSECURE_MODE")]
        internal string insecureMode { get; set; }

        public override string ToString()
        {
            return String.Format("{{ APPLICATION: {0}, OS_VERSION: {1}, NET_RUNTIME: {2}, NET_VERSION: {3}, INSECURE_MODE: {4} }}", 
                application, osVersion, netRuntime, netVersion, insecureMode);
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
