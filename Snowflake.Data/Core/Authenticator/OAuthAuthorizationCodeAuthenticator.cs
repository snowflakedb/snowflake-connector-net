using System;
using System.Net;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator.Browser;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Authenticator
{
    internal class OAuthAuthorizationCodeAuthenticator: OAuthFlowAuthenticator, IAuthenticator
    {
        public const string AuthName = "oauth_authorization_code";
        private const string BrowserSuccessResponse = @"<!DOCTYPE html><html><head><meta charset=""UTF-8""/>
<title>Authorization Code Granted for Snowflake</title></head>
<body><h4>Your identity was confirmed</h4>
Access to Snowflake has been granted to the .NET driver.
You can close this window now and go back where you started from.
</body></html>";
        private const string BrowserUnexpectedErrorResponse = "Retrieving Authorization Code response from Identity Provider failed";
        private const string BrowserErrorResponseTemplate = @"<!DOCTYPE html><html><head><meta charset=""UTF-8""/>
<title>Bad request</title></head>
<body><h4>Your identity was not confirmed because of errors</h4>
{error}
</body></html>";
        private const string DefaultRedirectUriHost = "127.0.0.1";
        private const int MaxRandomPortAcquisitionAttempts = 3;

        private static readonly TimeSpan s_idpRestTimeout = TimeSpan.FromSeconds(120);

        private readonly ChallengeProvider _challengeProvider;
        private readonly WebBrowserStarter _browserStarter;
        private readonly WebListenerStarter _listenerStarter;

        internal SecureString AccessToken { get; private set; } = null;

        public OAuthAuthorizationCodeAuthenticator(SFSession session) : base(session, OAuthAuthenticator.AUTH_NAME)
        {
            _challengeProvider = new ChallengeProvider();
            _browserStarter = WebBrowserStarter.Instance;
            _listenerStarter = WebListenerStarter.Instance;
        }

        internal OAuthAuthorizationCodeAuthenticator(SFSession session, ChallengeProvider challengeProvider, WebBrowserStarter browserStarter, WebListenerStarter listenerStarter) : this(session)
        {
            _challengeProvider = challengeProvider;
            _browserStarter = browserStarter;
            _listenerStarter = listenerStarter;
        }

        public static bool IsOAuthAuthorizationCodeAuthenticator(string authenticator) =>
            AuthName.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        public async Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            var cacheKeys = GetOAuthCacheKeys();
            var accessToken = cacheKeys.GetAccessToken();
            if (string.IsNullOrEmpty(accessToken))
            {
                var accessTokenRequest = RunFlowToAccessTokenRequest();
                await GetAccessTokenAsync(accessTokenRequest, cacheKeys, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                AccessToken = SecureStringHelper.Encode(accessToken);
            }

            try
            {
                await LoginAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (SnowflakeDbException exception)
            {
                if (ShouldRefreshToken(exception, cacheKeys))
                {
                    var refreshTokenRequest = PrepareRefreshTokenRequest(cacheKeys);
                    await GetAccessTokenAsync(refreshTokenRequest, cacheKeys, cancellationToken).ConfigureAwait(false);
                    await LoginAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }
                throw;
            }
        }

        public void Authenticate()
        {
            var cacheKeys = GetOAuthCacheKeys();
            var accessToken = cacheKeys.GetAccessToken();
            if (string.IsNullOrEmpty(accessToken))
            {
                var accessTokenRequest = RunFlowToAccessTokenRequest();
                GetAccessToken(accessTokenRequest, cacheKeys);
            }
            else
            {
                AccessToken = SecureStringHelper.Encode(accessToken);
            }

            try
            {
                Login();
            }
            catch (SnowflakeDbException exception)
            {
                if (ShouldRefreshToken(exception, cacheKeys))
                {
                    var refreshTokenRequest = PrepareRefreshTokenRequest(cacheKeys);
                    GetAccessToken(refreshTokenRequest, cacheKeys);
                    Login();
                    return;
                }
                throw;
            }
        }

        private void GetAccessToken(
            BaseOAuthAccessTokenRequest accessTokenRequest,
            OAuthCacheKeys cacheKeys)
        {
            var restRequester = (RestRequester) session.restRequester;
            using (var accessTokenHttpRequest = accessTokenRequest.CreateHttpRequest())
            {
                var restRequest = new RestRequestWrapper(accessTokenHttpRequest, s_idpRestTimeout);
                OAuthAccessTokenResponse accessTokenResponse = null;
                try
                {
                    accessTokenResponse = restRequester.Post<OAuthAccessTokenResponse>(restRequest);
                }
                catch (Exception exception)
                {
                    var realException = UnpackAggregateException(exception);
                    throw new SnowflakeDbException(SFError.OAUTH_TOKEN_REQUEST_ERROR, realException.Message);
                }
                HandleAccessTokenResponse(accessTokenResponse, cacheKeys);
            }
        }

        private async Task GetAccessTokenAsync(
            BaseOAuthAccessTokenRequest accessTokenRequest,
            OAuthCacheKeys cacheKeys,
            CancellationToken cancellationToken)
        {
            var restRequester = (RestRequester) session.restRequester;
            using (var accessTokenHttpRequest = accessTokenRequest.CreateHttpRequest())
            {
                var restRequest = new RestRequestWrapper(accessTokenHttpRequest, s_idpRestTimeout);
                OAuthAccessTokenResponse accessTokenResponse = null;
                try
                {
                    accessTokenResponse = await restRequester.PostAsync<OAuthAccessTokenResponse>(restRequest, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    var realException = UnpackAggregateException(exception);
                    throw new SnowflakeDbException(SFError.OAUTH_TOKEN_REQUEST_ERROR, realException.Message);
                }
                HandleAccessTokenResponse(accessTokenResponse, cacheKeys);
            }
        }

        private bool ShouldRefreshToken(SnowflakeDbException exception, OAuthCacheKeys cacheKeys) =>
            cacheKeys.IsAvailable() &&
            (OAuthTokenErrors.IsAccessTokenExpired(exception.ErrorCode) || OAuthTokenErrors.IsAccessTokenInvalid(exception.ErrorCode));

        private OAuthRefreshAccessTokenRequest PrepareRefreshTokenRequest(OAuthCacheKeys cacheKeys)
        {
            var refreshToken = cacheKeys.GetRefreshToken();
            cacheKeys.RemoveAccessToken();
            return new OAuthRefreshAccessTokenRequest
            {
                TokenEndpoint = GetTokenEndpoint(),
                ClientId = RequiredProperty(SFSessionProperty.OAUTHCLIENTID),
                ClientSecret = RequiredProperty(SFSessionProperty.OAUTHCLIENTSECRET),
                AuthorizationScope = GetAuthorizationScope(),
                RefreshToken = refreshToken
            };
        }

        private OAuthCacheKeys GetOAuthCacheKeys()
        {
            var host = new Uri(GetTokenEndpoint()).Host;
            var user = session.properties[SFSessionProperty.USER];
            return new OAuthCacheKeys(host, user, SnowflakeCredentialManagerFactory.GetCredentialManager);
        }

        private Exception UnpackAggregateException(Exception exception) =>
            exception is AggregateException ? ((AggregateException)exception).InnerException : exception;

        private OAuthAccessTokenRequest RunFlowToAccessTokenRequest()
        {
            var authorizationData = PrepareAuthorizationData();
            var authorizationCodeRequest = authorizationData.Request;
            var authorizationCodeResult = ExecuteAuthorizationCodeRequest(authorizationCodeRequest);
            return new OAuthAccessTokenRequest
            {
                TokenEndpoint = GetTokenEndpoint(),
                AuthorizationCode = authorizationCodeResult.AuthorizationCode,
                AuthorizationScope = authorizationCodeRequest.AuthorizationScope,
                ClientId = authorizationCodeRequest.ClientId,
                ClientSecret = RequiredProperty(SFSessionProperty.OAUTHCLIENTSECRET),
                CodeVerifier = authorizationData.Verifier.Value,
                RedirectUri = authorizationCodeRequest.RedirectUri
            };
        }

        internal AuthorizationRequestWithVerifier PrepareAuthorizationData()
        {
            var state = _challengeProvider.GenerateState();
            var codeVerifier = _challengeProvider.GenerateCodeVerifier();
            var codeChallenge = codeVerifier.ComputeCodeChallenge();
            var authorizationCodeRequest = new OAuthAuthorizationCodeRequest
            {
                AuthorizationEndpoint = GetAuthorizationEndpoint(),
                AuthorizationScope = GetAuthorizationScope(),
                ClientId = RequiredProperty(SFSessionProperty.OAUTHCLIENTID),
                RedirectUri = ExtractPropertyOrEmptyString(SFSessionProperty.OAUTHREDIRECTURI),
                CodeChallenge = codeChallenge,
                State = state
            };
            return new AuthorizationRequestWithVerifier
            {
                Request = authorizationCodeRequest,
                Verifier = codeVerifier
            };
        }

        private void HandleAccessTokenResponse(OAuthAccessTokenResponse accessTokenResponse, OAuthCacheKeys cacheKeys)
        {
            accessTokenResponse.Validate();
            var accessToken = accessTokenResponse.AccessToken;
            var refreshToken = accessTokenResponse.RefreshToken;
            cacheKeys.SaveAccessToken(accessToken);
            if (string.IsNullOrEmpty(refreshToken))
            {
                cacheKeys.RemoveRefreshToken();
            }
            else
            {
                cacheKeys.SaveRefreshToken(refreshToken);
            }
            AccessToken = SecureStringHelper.Encode(accessToken);
        }

        private OAuthAuthorizationCodeResponse ExecuteAuthorizationCodeRequest(OAuthAuthorizationCodeRequest request)
        {
            var timeoutInSec = int.Parse(session.properties[SFSessionProperty.BROWSER_RESPONSE_TIMEOUT]);
            var timeout = TimeSpan.FromSeconds(timeoutInSec);
            var extractor = new Func<HttpListenerRequest, Result<OAuthAuthorizationCodeResponse, IBrowserError>>(httpRequest => ValidateAndExtractAuthorizationCodeResult(httpRequest, request.State));
            using (var httpListener = StartListenerUpdatingRedirectUri(request))
            using (var browserListener = new WebBrowserListener<OAuthAuthorizationCodeResponse>(httpListener, extractor, BrowserSuccessResponse, BrowserUnexpectedErrorResponse))
            {
                var authorizationCodeUrlString = request.GetUrl();
                _browserStarter.StartBrowser(authorizationCodeUrlString);
                return browserListener.WaitAndGetResult(timeout);
            }
        }

        private Result<OAuthAuthorizationCodeResponse, IBrowserError> ValidateAndExtractAuthorizationCodeResult(HttpListenerRequest request, string expectedState)
        {
            if (request.HttpMethod != "GET")
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new AuthorizationCodeError
                {
                    BrowserMessage = BadRequestError("<br><b>Error</b>: Expected GET http method.</br>"),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_WRONG_METHOD, request.HttpMethod)
                });
            }

            if (string.IsNullOrEmpty(request.Url.Query))
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new AuthorizationCodeError
                {
                    BrowserMessage = BadRequestError("<br><b>Error</b>: No query parameters</br>"),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, "No query parameters")
                });
            }
            var parameters = HttpUtility.ParseQueryString(request.Url.Query);
            var error = parameters.Get("error");
            var errorDescription = parameters.Get("error_description");
            if (!string.IsNullOrEmpty(error) || !string.IsNullOrEmpty(errorDescription))
            {
                var errorMessage = "<br>Identity Provider failed with error: </br>"
                                   + $"<br><b>Error</b>: {HttpUtility.HtmlEncode(error)}</br>"
                                   + $"<br><b>Error Description</b>: {HttpUtility.HtmlEncode(errorDescription)}</br>";
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new AuthorizationCodeError
                {
                    BrowserMessage = BadRequestError(errorMessage),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, $"Authorization code response has error '{error}' and description '{errorDescription}'")
                });
            }
            var authorizationCode = parameters.Get("code");
            var state = parameters.Get("state");
            if (string.IsNullOrEmpty(authorizationCode))
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new AuthorizationCodeError
                {
                    BrowserMessage = BadRequestError("<br><b>Error</b>: Authorization code is required in the authorization code response</br>"),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, "Authorization code is required in the authorization code response")
                });
            }
            if (string.IsNullOrEmpty(state))
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new AuthorizationCodeError
                {
                    BrowserMessage = BadRequestError("<br><b>Error</b>: State is required in the authorization code response</br>"),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, "State is required in the authorization code response")
                });
            }
            if (state != expectedState)
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new AuthorizationCodeError
                {
                    BrowserMessage = "<br><b>Error</b>: Identity Provider did not provide expected state parameter! It might indicate an XSS attack.</br>",
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, "State mismatch for authorization code request and response")
                });
            }
            return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateResult(new OAuthAuthorizationCodeResponse
            {
                AuthorizationCode = authorizationCode,
                State = state
            });
        }

        private string BadRequestError(string error) => BrowserErrorResponseTemplate.Replace("{error}", error);

        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.OAuthType = AuthName;
            var secureAccessToken = AccessToken;
            data.Token = (secureAccessToken == null ? null : SecureStringHelper.Decode(secureAccessToken));
            if (string.IsNullOrEmpty(data.Token))
            {
                throw new Exception("No valid access token to use");
            }
            SetSecondaryAuthenticationData(ref data);
        }


        internal HttpListener StartListenerUpdatingRedirectUri(OAuthAuthorizationCodeRequest request)
        {
            if (!string.IsNullOrEmpty(request.RedirectUri))
                return _listenerStarter.StartHttpListener(AddSlashToNonEmptyUrl(request.RedirectUri));
            var listenerForDefaultRedirectUri = StartHttpListenerForDefaultRedirectUri();
            request.RedirectUri = listenerForDefaultRedirectUri.Uri;
            return listenerForDefaultRedirectUri.Listener;
        }

        private string AddSlashToNonEmptyUrl(string url)
        {
            if (string.IsNullOrEmpty(url))
                return url;
            return url.EndsWith("/") ? url : url + "/";
        }

        private ListenerWithUri StartHttpListenerForDefaultRedirectUri()
        {
            HttpListener listener = null;
            string redirectUri = null;
            var numberOfAttemptsLeft = MaxRandomPortAcquisitionAttempts;
            while (listener == null && numberOfAttemptsLeft > 0)
            {
                numberOfAttemptsLeft--;
                var port = _listenerStarter.GetRandomUnusedPort();
                redirectUri = $"http://{DefaultRedirectUriHost}:{port}";
                var redirectUriWithSlash = $"{redirectUri}/";
                try
                {
                    listener = _listenerStarter.StartHttpListener(redirectUriWithSlash);
                }
                catch (HttpListenerException)
                {
                    if (numberOfAttemptsLeft <= 0)
                        throw;
                }
            }
            return new ListenerWithUri
            {
                Listener = listener,
                Uri = redirectUri
            };
        }

        private string GetAuthorizationEndpoint()
        {
            var externalAuthUrl = ExtractPropertyOrEmptyString(SFSessionProperty.OAUTHAUTHORIZATIONURL);
            if (!string.IsNullOrEmpty(externalAuthUrl))
                return externalAuthUrl;
            return DefaultSnowflakeEndpoint(OAuthFlowConfig.SnowflakeAuthorizeUrl);
        }

        private string GetTokenEndpoint()
        {
            var externalTokenUrl = ExtractPropertyOrEmptyString(SFSessionProperty.OAUTHTOKENREQUESTURL);
            if (!string.IsNullOrEmpty(externalTokenUrl))
                return externalTokenUrl;
            return DefaultSnowflakeEndpoint(OAuthFlowConfig.SnowflakeTokenUrl);
        }

        private string GetAuthorizationScope()
        {
            var scope = ExtractPropertyOrEmptyString(SFSessionProperty.OAUTHSCOPE);
            if (!string.IsNullOrEmpty(scope))
                return scope;
            var role = RequiredProperty(SFSessionProperty.ROLE);
            return OAuthFlowConfig.DefaultScopePrefixBeforeRole + role;
        }

        private string DefaultSnowflakeEndpoint(string relativeUrl)
        {
            var host = RequiredProperty(SFSessionProperty.HOST);
            var scheme = RequiredProperty(SFSessionProperty.SCHEME);
            if (!scheme.Equals("https", StringComparison.OrdinalIgnoreCase))
            {
                throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, $"Property {SFSessionProperty.SCHEME.ToString()} was expected to be https");
            }
            var hostWithProtocol = host.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ? host : "https://" + host;
            var port = RequiredProperty(SFSessionProperty.PORT);
            return $"{hostWithProtocol}:{port}{relativeUrl}";
        }

        private string RequiredProperty(SFSessionProperty property)
        {
            var value = ExtractPropertyOrEmptyString(property);
            if (string.IsNullOrEmpty(value))
            {
                throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, $"Property {property.ToString()} is required for OAuth authorization code flow");
            }
            return value;
        }

        private string ExtractPropertyOrEmptyString(SFSessionProperty property) => ExtractPropertyOrDefault(property, string.Empty);

        private string ExtractPropertyOrDefault(SFSessionProperty property, string defaultValue)
        {
            if (session.properties.TryGetValue(property, out string value) && !string.IsNullOrEmpty(value))
                return value;
            return defaultValue;
        }

        private class ListenerWithUri
        {
            public HttpListener Listener { get; set; }
            public string Uri { get; set; }
        }

        internal class AuthorizationRequestWithVerifier
        {
            public OAuthAuthorizationCodeRequest Request { get; set; }
            public CodeVerifier Verifier { get; set; }
        }
    }
}
