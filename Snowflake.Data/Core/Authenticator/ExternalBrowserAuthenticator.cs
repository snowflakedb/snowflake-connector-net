using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using Snowflake.Data.Core.CredentialManager;
using System.Security;
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
        private static readonly byte[] SUCCESS_RESPONSE = System.Text.Encoding.UTF8.GetBytes(
            "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
            "<title> SAML Response for Snowflake </title></head>" +
            "<body>Your identity was confirmed and propagated to Snowflake .NET driver. You can close this window now and go back where you started from." +
            "</body></html>"
            );
        private static readonly byte[] ERROR_RESPONSE = System.Text.Encoding.UTF8.GetBytes(
            "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
            "<title> SAML Response for Snowflake </title></head>" +
            "<body>Authentication failed due to an error and was unable to extract a SAML response token." +
            "</body></html>"
            );
        // The saml token to send in the login request.
        private string _samlResponseToken;
        // The proof key to send in the login request.
        private string _proofKey;
        // Event for successful authentication.
        private ManualResetEvent _successEvent;
        // Placeholder in case an exception occurs while extracting the token from the browser response.
        private Exception _tokenExtractionException;

        internal string _idTokenKey = "";

        private SecureString _idToken;

        internal BrowserOperations _browserOperations = BrowserOperations.Instance;

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
                _idTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(
                    session.properties[SFSessionProperty.HOST],
                    user,
                    TokenType.IdToken);
            }
        }

        internal ExternalBrowserAuthenticator(SFSession session, BrowserOperations browserOperations) : this(session)
        {
            _browserOperations = browserOperations;
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
                int localPort = GetRandomUnusedPort();
                using (var httpListener = GetHttpListener(localPort))
                {
                    httpListener.Start();
                    logger.Debug("Get IdpUrl and ProofKey");
                    var loginUrl = await GetIdpUrlAndProofKeyAsync(localPort, cancellationToken).ConfigureAwait(false);
                    logger.Debug("Open browser");
                    StartBrowser(loginUrl);
                    logger.Debug("Get the redirect SAML request");
                    GetRedirectSamlRequest(httpListener);
                    httpListener.Stop();
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
                    throw e;
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
                int localPort = GetRandomUnusedPort();
                using (var httpListener = GetHttpListener(localPort))
                {
                    httpListener.Start();
                    logger.Debug("Get IdpUrl and ProofKey");
                    var loginUrl = GetIdpUrlAndProofKey(localPort);
                    logger.Debug("Open browser");
                    StartBrowser(loginUrl);
                    logger.Debug("Get the redirect SAML request");
                    GetRedirectSamlRequest(httpListener);
                    httpListener.Stop();
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
                    throw e;
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

        private void GetRedirectSamlRequest(HttpListener httpListener)
        {
            _successEvent = new ManualResetEvent(false);
            _tokenExtractionException = null;
            httpListener.BeginGetContext(GetContextCallback, httpListener);
            var timeoutInSec = int.Parse(session.properties[SFSessionProperty.BROWSER_RESPONSE_TIMEOUT]);
            if (!_successEvent.WaitOne(timeoutInSec * 1000))
            {
                logger.Error("Browser response timeout has been reached");
                throw new SnowflakeDbException(SFError.BROWSER_RESPONSE_TIMEOUT, timeoutInSec);
            }
            if (_tokenExtractionException != null)
            {
                throw _tokenExtractionException;
            }
        }

        private void GetContextCallback(IAsyncResult result)
        {
            HttpListener httpListener = (HttpListener)result.AsyncState;
            if (httpListener.IsListening)
            {
                HttpListenerContext context = null;
                try
                {
                    context = httpListener.EndGetContext(result);
                }
                catch (HttpListenerException ex)
                {
                    logger.Error("Error while trying to get context from HttpListener", ex);
                }
                if (context != null)
                {
                    HttpListenerRequest request = context.Request;

                    _samlResponseToken = ValidateAndExtractToken(request);
                    HttpListenerResponse response = context.Response;
                    try
                    {
                        using (var output = response.OutputStream)
                        {
                            if (!string.IsNullOrEmpty(_samlResponseToken))
                            {
                                output.Write(SUCCESS_RESPONSE, 0, SUCCESS_RESPONSE.Length);
                            }
                            else
                            {
                                output.Write(ERROR_RESPONSE, 0, ERROR_RESPONSE.Length);
                            }
                        }
                    }
                    catch
                    {
                        // Ignore the exception as it does not affect the overall authentication flow
                        logger.Warn("External browser response not sent out");
                    }
                }
            }

            _successEvent.Set();
        }

        private static int GetRandomUnusedPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        private static HttpListener GetHttpListener(int port)
        {
            HttpListener listener = new HttpListener();
            listener.Prefixes.Add($"http://{IPAddress.Loopback}:{port}/");
            listener.Prefixes.Add($"http://localhost:{port}/");
            return listener;
        }

        private void StartBrowser(string url)
        {
            string regexStr = "^http(s?)\\:\\/\\/[0-9a-zA-Z]([-.\\w]*[0-9a-zA-Z@:])*(:(0-9)*)*(\\/?)([a-zA-Z0-9\\-\\.\\?\\,\\&\\(\\)\\/\\\\\\+&%\\$#_=@]*)?$";
            Match m = Regex.Match(url, regexStr, RegexOptions.IgnoreCase);
            if (!m.Success || !Uri.IsWellFormedUriString(url, UriKind.Absolute))
            {
                logger.Error("Failed to start browser. Invalid url.");
                throw new SnowflakeDbException(SFError.INVALID_BROWSER_URL, url);
            }
            var uri = new Uri(url);
            if (url != uri.ToString())
            {
                logger.Error("Failed to start browser. Invalid uri.");
                throw new SnowflakeDbException(SFError.INVALID_BROWSER_URL, url);
            }
            _browserOperations.OpenUrl(uri);
        }

        private string ValidateAndExtractToken(HttpListenerRequest request)
        {
            if (request.HttpMethod != "GET")
            {
                logger.Error("Failed to extract token due to invalid HTTP method.");
                _tokenExtractionException = new SnowflakeDbException(SFError.BROWSER_RESPONSE_WRONG_METHOD, request.Url.Query);
                return null;
            }

            if (request.Url.Query == null || !request.Url.Query.StartsWith(TOKEN_REQUEST_PREFIX))
            {
                logger.Error("Failed to extract token due to invalid query.");
                _tokenExtractionException = new SnowflakeDbException(SFError.BROWSER_RESPONSE_INVALID_PREFIX, request.Url.Query);
                return null;
            }

            return Uri.UnescapeDataString(request.Url.Query.Substring(TOKEN_REQUEST_PREFIX.Length));
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
            Random rnd = new Random();
            Byte[] randomness = new Byte[32];
            rnd.NextBytes(randomness);
            return Convert.ToBase64String(randomness);
        }
    }
}
