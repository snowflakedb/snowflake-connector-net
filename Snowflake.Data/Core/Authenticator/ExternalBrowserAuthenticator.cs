using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Collections.Generic;
using Snowflake.Data.Core.CredentialManager;
using System.Security;
using System.Security.Cryptography;
using Snowflake.Data.Core.Authenticator.Browser;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// ExternalBrowserAuthenticator would start a new browser to perform authentication
    /// </summary>
    class ExternalBrowserAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AUTH_NAME = "externalbrowser";
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<ExternalBrowserAuthenticator>();
        private static readonly string TOKEN_REQUEST_PREFIX = "?token=";
        private const string OriginHeader = "Origin";
        private const string AccessControlRequestMethodHeader = "Access-Control-Request-Method";
        private const string AccessControlRequestHeadersHeader = "Access-Control-Request-Headers";
        private const string AccessControlAllowOriginHeader = "Access-Control-Allow-Origin";
        private const string AccessControlAllowMethodsHeader = "Access-Control-Allow-Methods";
        private const string AccessControlAllowHeadersHeader = "Access-Control-Allow-Headers";
        private const string AllowedCorsMethods = "POST, GET, OPTIONS";

        private static readonly string SuccessResponse =
            "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
            "<title> SAML Response for Snowflake </title></head>" +
            "<body>Your identity was confirmed and propagated to Snowflake .NET driver. You can close this window now and go back where you started from." +
            "</body></html>";

        private static readonly string ErrorResponse =
            "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
            "<title> SAML Response for Snowflake </title></head>" +
            "<body>Authentication failed due to an error and was unable to extract a SAML response token." +
            "</body></html>";

        // The saml token to send in the login request.
        private string _samlResponseToken;
        private string _postCallbackToken;
        // The proof key to send in the login request.
        private string _proofKey;

        internal string _idTokenKey = "";

        private SecureString _idToken;

        private readonly WebBrowserStarter _browserStarter = WebBrowserStarter.Instance;
        private readonly WebListenerStarter _listenerStarter = WebListenerStarter.Instance;

        /// <summary>
        /// Constructor of the External authenticator
        /// </summary>
        /// <param name="session"></param>
        internal ExternalBrowserAuthenticator(SFSession session) : base(session, AUTH_NAME)
        {
            var user = session.properties[SFSessionProperty.USER];
            var clientStoreTemporaryCredential = bool.Parse(session.properties[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL]);
            if (!string.IsNullOrEmpty(user) && clientStoreTemporaryCredential)
            {
                _idTokenKey = BuildIdTokenCacheKey();
            }
        }

        internal ExternalBrowserAuthenticator(SFSession session, IWebBrowserRunner browserRunner) : this(session)
        {
            _browserStarter = new WebBrowserStarter(browserRunner);
        }

        public static bool IsExternalBrowserAuthenticator(string authenticator) =>
            AUTH_NAME.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        /// <see cref="IAuthenticator"/>
        public async Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            logger.Info("External Browser Authentication");
            var idToken = string.IsNullOrEmpty(_idTokenKey) ? "" :
                SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(_idTokenKey);
            _idToken = string.IsNullOrEmpty(idToken) ? null : SecureStringHelper.Encode(idToken);
            if (_idToken == null)
            {
                int localPort = _listenerStarter.GetRandomUnusedPort();
                var localhostEndpoints = GetLocalhostEndpoints(localPort);
                using (var httpListener = _listenerStarter.StartHttpListener(localhostEndpoints))
                {
                    logger.Debug("Get IdpUrl and ProofKey");
                    var loginUrl = await GetIdpUrlAndProofKeyAsync(localPort, cancellationToken).ConfigureAwait(false);
                    logger.Debug("Get the redirect SAML request");
                    _samlResponseToken = GetRedirectSamlRequest(httpListener, loginUrl);
                }
            }

            logger.Debug("Send login request");
            try
            {
                await base.LoginAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (SnowflakeDbException e)
            {
                if (CheckIfTokenHasExpired(e))
                {
                    await AuthenticateAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    throw;
                }
            }
        }

        /// <see cref="IAuthenticator"/>
        public void Authenticate()
        {
            logger.Info("External Browser Authentication");
            var idToken = string.IsNullOrEmpty(_idTokenKey) ? "" :
                SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(_idTokenKey);
            _idToken = string.IsNullOrEmpty(idToken) ? null : SecureStringHelper.Encode(idToken);
            if (_idToken == null)
            {
                int localPort = _listenerStarter.GetRandomUnusedPort();
                var localhostEndpoints = GetLocalhostEndpoints(localPort);
                using (var httpListener = _listenerStarter.StartHttpListener(localhostEndpoints))
                {
                    logger.Debug("Get IdpUrl and ProofKey");
                    var loginUrl = GetIdpUrlAndProofKey(localPort);
                    logger.Debug("Get the redirect SAML request");
                    _samlResponseToken = GetRedirectSamlRequest(httpListener, loginUrl);
                }
            }

            logger.Debug("Send login request");
            try
            {
                base.Login();
            }
            catch (SnowflakeDbException e)
            {
                if (CheckIfTokenHasExpired(e))
                {
                    Authenticate();
                }
                else
                {
                    throw;
                }
            }
        }

        private bool CheckIfTokenHasExpired(SnowflakeDbException e)
        {
            if (e.ErrorCode == SFError.ID_TOKEN_INVALID.GetAttribute<SFErrorAttr>().errorCode)
            {
                logger.Info("SSO Token has expired or not valid. Reauthenticating without SSO token...", e);
                SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(_idTokenKey);
                return true;
            }
            return false;
        }

        private string GetIdpUrlAndProofKey(int localPort)
        {
            if (session._disableConsoleLogin)
            {
                var authenticatorRestRequest = BuildAuthenticatorRestRequest(localPort);
                var authenticatorRestResponse = session.restRequester.Post<AuthenticatorResponse>(authenticatorRestRequest);
                authenticatorRestResponse.FilterFailedResponse();

                _proofKey = authenticatorRestResponse.data.proofKey;
                return authenticatorRestResponse.data.ssoUrl;
            }
            else
            {
                _proofKey = GenerateProofKey();
                return GetLoginUrl(_proofKey, localPort);
            }
        }

        private async Task<string> GetIdpUrlAndProofKeyAsync(int localPort, CancellationToken cancellationToken)
        {
            if (session._disableConsoleLogin)
            {
                var authenticatorRestRequest = BuildAuthenticatorRestRequest(localPort);
                var authenticatorRestResponse =
                    await session.restRequester.PostAsync<AuthenticatorResponse>(
                        authenticatorRestRequest,
                        cancellationToken
                    ).ConfigureAwait(false);
                authenticatorRestResponse.FilterFailedResponse();

                _proofKey = authenticatorRestResponse.data.proofKey;
                return authenticatorRestResponse.data.ssoUrl;
            }
            else
            {
                _proofKey = GenerateProofKey();
                return GetLoginUrl(_proofKey, localPort);
            }
        }

        private string GetRedirectSamlRequest(HttpListener httpListener, string loginUrl)
        {
            var timeoutInSec = int.Parse(session.properties[SFSessionProperty.BROWSER_RESPONSE_TIMEOUT]);
            var timeout = TimeSpan.FromSeconds(timeoutInSec);
            var accountUrl = session.BuildUri(string.Empty);
            logger.Debug($"External browser callback account origin: {accountUrl.Scheme}://{accountUrl.Host}:{accountUrl.Port}");
            using (var browserListener = new WebBrowserListener<ExternalBrowserToken>(
                httpListener,
                context => HandleCallbackRequest(context, accountUrl)
                    ? null
                    : ValidateAndExtractToken(context.Request),
                SuccessResponse,
                ErrorResponse))
            {
                logger.Debug("Open browser");
                _browserStarter.StartBrowser(new Url(loginUrl));
                return browserListener.WaitAndGetResult(timeout).Token;
            }
        }

        private static string[] GetLocalhostEndpoints(int port) =>
            new[] { $"http://{IPAddress.Loopback}:{port}/", $"http://localhost:{port}/" };

        private bool HandleCallbackRequest(HttpListenerContext context, Uri accountUrl)
        {
            var request = context.Request;
            var origin = request.Headers[OriginHeader];

            if (request.HttpMethod.Equals(HttpMethod.Options.Method, StringComparison.OrdinalIgnoreCase))
            {
                HandlePreflight(context.Response, origin, request.Headers, accountUrl);
                return true;
            }

            var isOriginless = string.IsNullOrEmpty(origin) || string.Equals(origin, "null", StringComparison.OrdinalIgnoreCase);
            var isPost = request.HttpMethod.Equals(HttpMethod.Post.Method, StringComparison.OrdinalIgnoreCase);
            if ((isPost || !isOriginless) && !OriginMatchesAccount(origin, accountUrl))
            {
                logger.Warn("Ignoring external browser callback with an unexpected Origin.");
                CloseResponse(context.Response, HttpStatusCode.Forbidden);
                return true;
            }

            if (isPost)
            {
                var postToken = TryExtractTokenFromPost(request);
                if (string.IsNullOrEmpty(postToken))
                {
                    logger.Warn("Ignoring external browser callback POST without a token.");
                    CloseResponse(context.Response, HttpStatusCode.OK);
                    return true;
                }

                _postCallbackToken = postToken;
                ApplyCorsOrigin(context.Response, origin, isOriginless);
                return false;
            }

            if (!request.HttpMethod.Equals(HttpMethod.Get.Method, StringComparison.OrdinalIgnoreCase))
            {
                logger.Warn("Ignoring external browser callback with an unexpected HTTP method.");
                CloseResponse(context.Response, HttpStatusCode.MethodNotAllowed);
                return true;
            }

            if (!HasTokenQueryParameter(request))
            {
                logger.Warn("Ignoring external browser callback GET without a token.");
                CloseResponse(context.Response, HttpStatusCode.OK);
                return true;
            }

            ApplyCorsOrigin(context.Response, origin, isOriginless);
            return false;
        }

        private static void ApplyCorsOrigin(HttpListenerResponse response, string origin, bool isOriginless)
        {
            if (isOriginless)
            {
                return;
            }

            response.Headers[AccessControlAllowOriginHeader] = origin;
            response.Headers["Vary"] = OriginHeader;
        }

        private static void HandlePreflight(HttpListenerResponse response, string origin, NameValueCollection headers, Uri accountUrl)
        {
            var requestedMethod = headers[AccessControlRequestMethodHeader];
            var requestedHeaders = headers[AccessControlRequestHeadersHeader];
            if (!OriginMatchesAccount(origin, accountUrl) ||
                !string.Equals(requestedMethod, HttpMethod.Post.Method, StringComparison.OrdinalIgnoreCase) ||
                !AreRequestedHeadersAllowed(requestedHeaders))
            {
                CloseResponse(response, HttpStatusCode.BadRequest);
                return;
            }

            response.StatusCode = (int)HttpStatusCode.NoContent;
            response.Headers[AccessControlAllowOriginHeader] = origin;
            response.Headers[AccessControlAllowMethodsHeader] = AllowedCorsMethods;
            response.Headers["Vary"] = OriginHeader;
            if (!string.IsNullOrWhiteSpace(requestedHeaders))
            {
                response.Headers[AccessControlAllowHeadersHeader] = string.Join(
                    ", ",
                    requestedHeaders.Split(',').Select(header => header.Trim()).Where(header => header.Length > 0));
            }
            response.Close();
        }

        private static void CloseResponse(HttpListenerResponse response, HttpStatusCode statusCode)
        {
            response.StatusCode = (int)statusCode;
            response.ContentLength64 = 0;
            response.Close();
        }

        private static bool AreRequestedHeadersAllowed(string requestedHeaders) =>
            string.IsNullOrWhiteSpace(requestedHeaders) ||
            requestedHeaders.Split(',')
                .Select(header => header.Trim())
                .Where(header => header.Length > 0)
                .All(header => string.Equals(header, "Content-Type", StringComparison.OrdinalIgnoreCase));

        internal static bool OriginMatchesAccount(string origin, Uri accountUrl)
        {
            if (string.IsNullOrEmpty(origin) ||
                string.Equals(origin, "null", StringComparison.OrdinalIgnoreCase) ||
                !Uri.TryCreate(origin, UriKind.Absolute, out var originUrl) ||
                !string.IsNullOrEmpty(originUrl.UserInfo) ||
                originUrl.AbsolutePath != "/" ||
                !string.IsNullOrEmpty(originUrl.Query) ||
                !string.IsNullOrEmpty(originUrl.Fragment))
            {
                return false;
            }

            return string.Equals(originUrl.Scheme, accountUrl.Scheme, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(originUrl.IdnHost, accountUrl.IdnHost, StringComparison.OrdinalIgnoreCase) &&
                originUrl.Port == accountUrl.Port;
        }

        private Result<ExternalBrowserToken, IBrowserError> ValidateAndExtractToken(HttpListenerRequest request)
        {
            if (!string.IsNullOrEmpty(_postCallbackToken))
            {
                var postToken = _postCallbackToken;
                _postCallbackToken = null;
                return Result<ExternalBrowserToken, IBrowserError>.CreateResult(new ExternalBrowserToken(postToken));
            }

            if (request.HttpMethod != HttpMethod.Get.Method)
            {
                logger.Error("Failed to extract token due to invalid HTTP method.");
                return Result<ExternalBrowserToken, IBrowserError>.CreateError(new BrowserError
                {
                    BrowserMessage = ErrorResponse,
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_WRONG_METHOD, request.HttpMethod)
                });
            }

            if (request.Url.Query == null || !request.Url.Query.StartsWith(TOKEN_REQUEST_PREFIX))
            {
                logger.Error("Failed to extract token due to invalid query.");
                return Result<ExternalBrowserToken, IBrowserError>.CreateError(new BrowserError
                {
                    BrowserMessage = ErrorResponse,
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_INVALID_PREFIX, request.Url.Query)
                });
            }

            var token = Uri.UnescapeDataString(request.Url.Query.Substring(TOKEN_REQUEST_PREFIX.Length));
            if (string.IsNullOrEmpty(token))
            {
                return Result<ExternalBrowserToken, IBrowserError>.CreateError(new BrowserError
                {
                    BrowserMessage = ErrorResponse,
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, "could not retrieve token")
                });
            }
            return Result<ExternalBrowserToken, IBrowserError>.CreateResult(new ExternalBrowserToken(token));
        }

        private static bool HasTokenQueryParameter(HttpListenerRequest request) =>
            request.QueryString["token"] != null;

        internal static string TryExtractTokenFromPost(string body)
        {
            if (string.IsNullOrEmpty(body))
            {
                return null;
            }

            try
            {
                var payload = JsonConvert.DeserializeObject<ExternalBrowserPostPayload>(body);
                if (payload != null && !string.IsNullOrEmpty(payload.Token))
                {
                    return payload.Token;
                }
            }
            catch (JsonException e)
            {
                logger.Warn("POST callback body is not valid JSON; falling back to form-encoded parsing.", e);
            }

            var formToken = HttpUtility.ParseQueryString(body)["token"];
            return string.IsNullOrEmpty(formToken) ? null : formToken;
        }

        private static string TryExtractTokenFromPost(HttpListenerRequest request)
        {
            if (!request.HasEntityBody)
            {
                return null;
            }

            using (var reader = new StreamReader(request.InputStream, request.ContentEncoding))
            {
                return TryExtractTokenFromPost(reader.ReadToEnd());
            }
        }

        private class ExternalBrowserPostPayload
        {
            [JsonProperty(PropertyName = "token")]
            public string Token { get; set; }

            [JsonProperty(PropertyName = "consent")]
            public bool? Consent { get; set; }
        }

        private SFRestRequest BuildAuthenticatorRestRequest(int port)
        {
            var fedUrl = session.BuildUri(RestPath.SF_AUTHENTICATOR_REQUEST_PATH);
            var data = new AuthenticatorRequestData()
            {
                AccountName = session.properties[SFSessionProperty.ACCOUNT],
                Authenticator = AUTH_NAME,
                BrowserModeRedirectPort = port.ToString(),
                DriverName = SFEnvironment.DriverName,
                DriverVersion = SFEnvironment.DriverVersion,
            };

            int connectionTimeoutSec = int.Parse(session.properties[SFSessionProperty.CONNECTION_TIMEOUT]);

            return session.BuildTimeoutRestRequest(fedUrl, new AuthenticatorRequest() { Data = data });
        }

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            if (_idToken == null)
            {
                // Add the token and proof key to the Data
                data.Token = _samlResponseToken;
                data.ProofKey = _proofKey;
            }
            else
            {
                data.Token = SecureStringHelper.Decode(_idToken);
                data.Authenticator = TokenType.IdToken.GetAttribute<StringAttr>().value;
            }
            SetSecondaryAuthenticationData(ref data);
        }

        private string GetLoginUrl(string proofKey, int localPort)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>()
            {
                { "login_name", session.properties[SFSessionProperty.USER]},
                { "proof_key", proofKey },
                { "browser_mode_redirect_port", localPort.ToString() }
            };
            Uri loginUrl = session.BuildUri(RestPath.SF_CONSOLE_LOGIN, parameters);
            return loginUrl.ToString();
        }

        private string GenerateProofKey()
        {
            Byte[] randomness = new Byte[32];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(randomness);
            }
            return Convert.ToBase64String(randomness);
        }

        private string BuildIdTokenCacheKey() =>
            SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                TokenType: TokenType.IdToken,
                Idp: "",
                SnowflakeUrl: session.properties[SFSessionProperty.HOST],
                Username: session.properties[SFSessionProperty.USER],
                Role: ""));
    }
}
