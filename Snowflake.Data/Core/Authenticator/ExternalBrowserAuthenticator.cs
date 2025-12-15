using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Collections.Generic;
using Snowflake.Data.Core.CredentialManager;
using System.Security;
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
                _idTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(
                    session.properties[SFSessionProperty.HOST],
                    user,
                    TokenType.IdToken);
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
            var extractor = new Func<HttpListenerRequest, Result<ExternalBrowserToken, IBrowserError>>(ValidateAndExtractToken);
            using (var browserListener = new WebBrowserListener<ExternalBrowserToken>(httpListener, extractor, SuccessResponse, ErrorResponse))
            {
                logger.Debug("Open browser");
                _browserStarter.StartBrowser(new Url(loginUrl));
                return browserListener.WaitAndGetResult(timeout).Token;
            }
        }

        private static string[] GetLocalhostEndpoints(int port) =>
            new[] { $"http://{IPAddress.Loopback}:{port}/", $"http://localhost:{port}/" };

        private Result<ExternalBrowserToken, IBrowserError> ValidateAndExtractToken(HttpListenerRequest request)
        {
            if (request.HttpMethod != "GET")
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
