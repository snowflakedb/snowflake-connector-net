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
        public static readonly int s_defaultRestRetrySecondsTimeout = 120;

        // Default each http request timeout to 16 seconds
        public static readonly int s_defaultHttpSecondsTimeout = 16;

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
            message.SetOption(HTTP_REQUEST_TIMEOUT_KEY, HttpTimeout);
            message.SetOption(REST_REQUEST_TIMEOUT_KEY, RestTimeout);
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
            }
            else
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
            RestTimeout = TimeSpan.FromSeconds(s_defaultRestRetrySecondsTimeout);
            HttpTimeout = TimeSpan.FromSeconds(s_defaultHttpSecondsTimeout);
        }

        internal Object jsonBody { get; set; }

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
                var json = JsonConvert.SerializeObject(jsonBody, JsonUtils.JsonSettings);
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

        [JsonProperty(PropertyName = "CLIENT_APP_ID", NullValueHandling = NullValueHandling.Ignore)]
        internal String DriverName { get; set; }

        [JsonProperty(PropertyName = "CLIENT_APP_VERSION", NullValueHandling = NullValueHandling.Ignore)]
        internal String DriverVersion { get; set; }

        public override string ToString()
        {
            return String.Format("AuthenticatorRequestData {{ACCOUNT_NAME: {0} }}",
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

        [JsonProperty(PropertyName = "EXT_AUTHN_DUO_METHOD", NullValueHandling = NullValueHandling.Ignore)]
        internal string extAuthnDuoMethod { get; set; }

        [JsonProperty(PropertyName = "PASSCODE", NullValueHandling = NullValueHandling.Ignore)]
        internal string passcode;

        [JsonProperty(PropertyName = "PROVIDER", NullValueHandling = NullValueHandling.Ignore)]
        internal string Provider { get; set; }

        [JsonProperty(PropertyName = "SESSION_PARAMETERS", NullValueHandling = NullValueHandling.Ignore)]
        internal Dictionary<SFSessionParameter, Object> SessionParameters { get; set; }

        [JsonIgnore]
        internal TimeSpan? HttpTimeout { get; set; }

        public override string ToString()
        {
            return String.Format("LoginRequestData {{ClientAppVersion: {0},\n AccountName: {1},\n loginName: {2},\n ClientEnv: {3},\n authenticator: {4} }}",
                clientAppVersion, accountName, loginName, clientEnv.ToString(), Authenticator);
        }
    }

    internal class LoginRequestClientEnv
    {
        [JsonProperty(PropertyName = "APPLICATION")]
        internal String application { get; set; }

        [JsonProperty(PropertyName = "OS_VERSION")]
        internal String osVersion { get; set; }

        [JsonProperty(PropertyName = "NET_RUNTIME")]
        internal String netRuntime { get; set; }

        [JsonProperty(PropertyName = "NET_VERSION")]
        internal string netVersion { get; set; }

        [JsonProperty(PropertyName = "CERT_REVOCATION_CHECK_MODE")]
        internal string certRevocationCheckMode { get; set; }

        [JsonProperty(PropertyName = "OAUTH_TYPE")]
        internal string oauthType { get; set; }

        [JsonProperty(PropertyName = "APPLICATION_PATH")]
        internal string applicationPath { get; set; }

        [JsonProperty(PropertyName = "CORE_VERSION", NullValueHandling = NullValueHandling.Ignore)]
        internal string minicoreVersion { get; set; }

        [JsonProperty(PropertyName = "CORE_FILE_NAME", NullValueHandling = NullValueHandling.Ignore)]
        internal string minicoreFileName { get; set; }

        [JsonProperty(PropertyName = "CORE_LOAD_ERROR", NullValueHandling = NullValueHandling.Ignore)]
        internal string minicoreLoadError { get; set; }

        [JsonProperty(PropertyName = "ISA", NullValueHandling = NullValueHandling.Ignore)]
        internal string isa { get; set; }

        [JsonIgnore]
        internal string processName { get; set; }

        [JsonIgnore]
        internal bool IsNetFramework => netRuntime.Contains("NETFramework");

        public override string ToString()
        {
            return String.Format("{{ APPLICATION: {0}, OS_VERSION: {1}, NET_RUNTIME: {2}, NET_VERSION: {3}, CERT_REVOCATION_CHECK_MODE: {4}, APPLICATION_PATH: {5} }}",
                application, osVersion, netRuntime, netVersion, certRevocationCheckMode, applicationPath);
        }

        public LoginRequestClientEnv CloneForSession()
        {
            return new LoginRequestClientEnv()
            {
                osVersion = osVersion,
                netRuntime = netRuntime,
                netVersion = netVersion,
                processName = processName,
                applicationPath = applicationPath,
                isa = isa,
                minicoreVersion = SFEnvironment.MinicoreDisabled ? null : MiniCore.SfMiniCore.TryGetVersionSafe(),
                minicoreFileName = SFEnvironment.MinicoreDisabled ? null : MiniCore.SfMiniCore.GetExpectedLibraryName(),
                minicoreLoadError = SFEnvironment.MinicoreDisabled
                    ? MiniCore.SfMiniCore.DISABLED_MESSAGE
                    : MiniCore.SfMiniCore.GetLoadError()
            };
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

        [JsonProperty(PropertyName = "bindStage")]
        internal string bindStage { get; set; }

        [JsonProperty(PropertyName = "parameters")]
        internal Dictionary<string, string> parameters { get; set; }

        [JsonProperty(PropertyName = "queryContextDTO", NullValueHandling = NullValueHandling.Ignore)]
        internal RequestQueryContext QueryContextDTO { get; set; }

        [JsonProperty(PropertyName = "asyncExec")]
        internal bool asyncExec { get; set; }
    }

    // The query context in query response
    internal class RequestQueryContext
    {
        [JsonProperty(PropertyName = "entries")]
        internal List<RequestQueryContextElement> Entries { get; set; }
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
        [JsonProperty(PropertyName = "base64Data")]
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
        [JsonProperty(PropertyName = "id")]
        public long Id { get; set; }

        // When the query context read (bigint). Compare for same id.
        [JsonProperty(PropertyName = "timestamp")]
        public long ReadTimestamp { get; set; }

        // Priority of the query context (bigint). Compare for different ids.
        [JsonProperty(PropertyName = "priority")]
        public long Priority { get; set; }

        // Opaque information (object with a value of base64 encoded string).
        [JsonProperty(PropertyName = "context")]
        public object Context { get; set; }

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
