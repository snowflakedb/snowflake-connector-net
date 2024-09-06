using System.Net.Http;
using System.Net.Http.Headers;
using System;
using System.Text;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Snowflake.Data.Core
{
    internal interface IRestRequest
    {
        HttpRequestMessage ToRequestMessage(HttpMethod method);
        TimeSpan GetRestTimeout();
        string getSid();
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

        internal String sid { get; set; }

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

        string IRestRequest.getSid()
        {
            return sid;
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
        private static MediaTypeWithQualityHeaderValue applicationJson = new MediaTypeWithQualityHeaderValue("application/json");

        private const string SF_AUTHORIZATION_HEADER = "Authorization";
        private const string SF_SERVICE_NAME_HEADER = "X-Snowflake-Service";

        private const string ClientAppId = "CLIENT_APP_ID";
        private const string ClientAppVersion = "CLIENT_APP_VERSION";

        internal SFRestRequest() : base()
        {
            RestTimeout = TimeSpan.FromSeconds(DEFAULT_REST_RETRY_SECONDS_TIMEOUT);

            // default each http request timeout to 16 seconds
            HttpTimeout = TimeSpan.FromSeconds(16);
        }

        internal Object jsonBody { get; set;  }

        internal String authorizationToken { get; set; }

        internal String serviceName { get; set; }

        internal bool isPutGet { get; set; }

        internal bool _isLogin { get; set; }

        internal bool _isStatusRequest { get; set; }

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
                var json = JsonSerializer.Serialize(jsonBody, JsonUtils.JsonOptions);
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

            if (isPutGet || _isStatusRequest)
            {
                message.Headers.Accept.Add(applicationJson);
            }
            else
            {
                message.Headers.Accept.Add(applicationSnowflake);
            }

            if (_isLogin)
            {
                message.Headers.Add(ClientAppId, SFEnvironment.DriverName);
                message.Headers.Add(ClientAppVersion, SFEnvironment.DriverVersion);
            }

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
        [JsonPropertyName("data")]
        internal AuthenticatorRequestData Data { get; set; }

        public override string ToString()
        {
            return String.Format("AuthenticatorRequest {{data: {0} }}", Data.ToString());
        }
    }

    class AuthenticatorRequestData
    {
        [JsonPropertyName("ACCOUNT_NAME")]
        internal String AccountName { get; set; }

        [JsonPropertyName("AUTHENTICATOR")]
        internal String Authenticator { get; set; }

        [JsonPropertyName("BROWSER_MODE_REDIRECT_PORT")]
        internal String BrowserModeRedirectPort { get; set; }

        [JsonPropertyName("CLIENT_APP_ID")]
        internal String DriverName { get; set; }

        [JsonPropertyName("CLIENT_APP_VERSION")]
        internal String DriverVersion { get; set; }

        public override string ToString()
        {
            return String.Format("AuthenticatorRequestData {{ACCOUNT_NAME: {0} }}",
                AccountName.ToString());
        }
    }

    class LoginRequest
    {
        [JsonPropertyName("data")]
        public LoginRequestData data { get; set; }

        public override string ToString()
        {
            return String.Format("LoginRequest {{data: {0} }}", data.ToString());
        }
    }

    class LoginRequestData
    {
        [JsonPropertyName("CLIENT_APP_ID")]
        public String clientAppId { get; set; }

        [JsonPropertyName("CLIENT_APP_VERSION")]
        public String clientAppVersion { get; set; }

        [JsonPropertyName("ACCOUNT_NAME")]
        public String accountName { get; set; }

        [JsonPropertyName("LOGIN_NAME")]
        public String loginName { get; set; }

        [JsonPropertyName("PASSWORD")]
        public String password { get; set; }

        [JsonPropertyName("AUTHENTICATOR")]
        public String Authenticator { get; set; }

        [JsonPropertyName("CLIENT_ENVIRONMENT")]
        public LoginRequestClientEnv clientEnv { get; set; }

        [JsonPropertyName("RAW_SAML_RESPONSE")]
        public String RawSamlResponse { get; set; }

        [JsonPropertyName("TOKEN")]
        public string Token { get; set; }

        [JsonPropertyName("PROOF_KEY")]
        public string ProofKey { get; set; }

        [JsonPropertyName("SESSION_PARAMETERS")]
        public Dictionary<SFSessionParameter, Object> SessionParameters { get; set; }

        public override string ToString()
        {
            return String.Format("LoginRequestData {{ClientAppVersion: {0},\n AccountName: {1},\n loginName: {2},\n ClientEnv: {3},\n authenticator: {4} }}",
                clientAppVersion, accountName, loginName, clientEnv.ToString(), Authenticator);
        }
    }

    class LoginRequestClientEnv
    {
        [JsonPropertyName("APPLICATION")]
        public String application { get; set; }

        [JsonPropertyName("OS_VERSION")]
        public String osVersion { get; set; }

        [JsonPropertyName("NET_RUNTIME")]
        public String netRuntime { get; set; }

        [JsonPropertyName("NET_VERSION")]
        public string netVersion { get; set; }

        [JsonPropertyName("INSECURE_MODE")]
        public string insecureMode { get; set; }

        [JsonIgnore]
        internal bool IsNetFramework => netRuntime.Contains("NETFramework");

        public override string ToString()
        {
            return String.Format("{{ APPLICATION: {0}, OS_VERSION: {1}, NET_RUNTIME: {2}, NET_VERSION: {3}, INSECURE_MODE: {4} }}",
                application, osVersion, netRuntime, netVersion, insecureMode);
        }
    }

    class QueryRequest
    {
        [JsonPropertyName("sqlText")]
        public string sqlText { get; set; }

        [JsonPropertyName("describeOnly")]
        public bool describeOnly { get; set; }

        [JsonPropertyName("bindings")]
        public Dictionary<string, BindingDTO> parameterBindings { get; set; }

        [JsonPropertyName("bindStage")]
        public string bindStage { get; set; }

        [JsonPropertyName("parameters")]
        public Dictionary<string, string> parameters { get; set; }

        [JsonPropertyName("queryContextDTO")]
        public RequestQueryContext QueryContextDTO { get; set; }

        [JsonPropertyName("asyncExec")]
        public bool asyncExec { get; set; }
    }

    // The query context in query response
    internal class RequestQueryContext
    {
        [JsonPropertyName("entries")]
        public List<RequestQueryContextElement> Entries { get; set; }
    }

    // The empty query context value in request
    internal class QueryContextValueEmpty
    {
        // empty object with no filed
    }

    // The non-empty query context value in request
    internal class QueryContextValue
    {
        // base64 encoded string of Opaque information
        [JsonPropertyName("base64Data")]
        public string Base64Data { get; set; }

        public QueryContextValue(string context)
        {
            Base64Data = context;
        }
    }

    // The query context in query response
    internal class RequestQueryContextElement
    {
        // database id as key. (bigint)
        [JsonPropertyName("id")]
        public long Id { get; set; }

        // When the query context read (bigint). Compare for same id.
        [JsonPropertyName("timestamp")]
        public long ReadTimestamp { get; set; }

        // Priority of the query context (bigint). Compare for different ids.
        [JsonPropertyName("priority")]
        public long Priority { get; set; }

        // Opaque information (object with a value of base64 encoded string).
        [JsonPropertyName("context")]
        public object Context{ get; set; }

        public void SetContext(string context)
        {
            if (context != null)
            {
                Context = new QueryContextValue(context);
            }
            else
            {
                Context = new QueryContextValueEmpty();
            }
        }

        // default constructor for JSON converter
        public RequestQueryContextElement() { }

        public RequestQueryContextElement(QueryContextElement elem)
        {
            Id = elem.Id;
            Priority = elem.Priority;
            ReadTimestamp = elem.ReadTimestamp;
            SetContext(elem.Context);
        }
    }

    class QueryCancelRequest
    {
        [JsonPropertyName("requestId")]
        public string requestId { get; set; }
    }

    class RenewSessionRequest
    {
        [JsonPropertyName("oldSessionToken")]
        public string oldSessionToken { get; set; }

        [JsonPropertyName("requestType")]
        public string requestType { get; set; }
    }
}
