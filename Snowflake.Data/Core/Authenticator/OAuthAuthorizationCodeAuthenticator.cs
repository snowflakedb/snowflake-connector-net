using System;
using System.Net;
using System.Web;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator.Browser;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    internal class OAuthAuthorizationCodeAuthenticator : OAuthFlowAuthenticator, IAuthenticator
    {
        public const string AuthName = "oauth_authorization_code";
        private const string TokenRequestGrantType = "authorization_code";
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

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OAuthAuthorizationCodeAuthenticator>();

        private readonly ChallengeProvider _challengeProvider;
        private readonly WebBrowserStarter _browserStarter;
        private readonly WebListenerStarter _listenerStarter;


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

        protected override string GetAuthenticatorName() => AuthName;

        protected override OAuthCacheKeys GetOAuthCacheKeys()
        {
            var host = new Uri(GetTokenEndpoint()).Host;
            var user = session.properties[SFSessionProperty.USER];
            var clientStoreTemporaryCredentials = bool.Parse(session.properties[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL]);
            return OAuthCacheKeys.CreateForAuthorizationCodeFlow(host, user, clientStoreTemporaryCredentials, SnowflakeCredentialManagerFactory.GetCredentialManager);
        }

        protected override OAuthAccessTokenRequest RunFlowToAccessTokenRequest()
        {
            var authorizationData = PrepareAuthorizationData();
            var authorizationCodeRequest = authorizationData.Request;
            var authorizationCodeResult = ExecuteAuthorizationCodeRequest(authorizationCodeRequest);
            s_logger.Debug($"Received authorization code from {authorizationCodeRequest.AuthorizationEndpoint}");
            return new OAuthAccessTokenRequest
            {
                TokenEndpoint = GetTokenEndpoint(),
                GrantType = TokenRequestGrantType,
                AuthorizationCode = authorizationCodeResult.AuthorizationCode,
                AuthorizationScope = authorizationCodeRequest.AuthorizationScope,
                ClientId = authorizationCodeRequest.ClientId,
                ClientSecret = RequiredProperty(SFSessionProperty.OAUTHCLIENTSECRET),
                CodeVerifier = authorizationData.Verifier.Value,
                RedirectUri = authorizationCodeRequest.RedirectUri,
                EnableSingleUseRefreshTokens = GetEnableSingleUseRefreshTokens()
            };
        }

        private string GetEnableSingleUseRefreshTokens()
        {
            var enableSingleUseRefreshTokensString = ExtractPropertyOrEmptyString(SFSessionProperty.OAUTHENABLESINGLEUSEREFRESHTOKENS);
            if (string.IsNullOrEmpty(enableSingleUseRefreshTokensString))
                return null;
            var enableSingleUseRefreshTokens = bool.Parse(enableSingleUseRefreshTokensString);
            return enableSingleUseRefreshTokens ? "true" : null;
        }

        protected override BaseOAuthAccessTokenRequest GetRenewAccessTokenRequest(SnowflakeDbException exception, OAuthCacheKeys cacheKeys)
        {
            if (!IsAccessTokenExpiredOrInvalid(exception))
            {
                s_logger.Debug($"Exception code returned for {AuthName} authentication does not indicate expired or invalid token so the authentication flow is failing");
                return null;
            }
            if (!cacheKeys.IsAvailable())
            {
                s_logger.Debug($"Cache in this {AuthName} authentication is disabled so there won't be any attempts to use a refresh token");
                return null;
            }
            var refreshTokenRequest = BuildRefreshTokenRequest(cacheKeys);
            if (refreshTokenRequest != null)
            {
                s_logger.Debug("Refresh token is going to be used to refresh the access token");
            }
            return refreshTokenRequest;
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

        private OAuthAuthorizationCodeResponse ExecuteAuthorizationCodeRequest(OAuthAuthorizationCodeRequest request)
        {
            var timeoutInSec = int.Parse(session.properties[SFSessionProperty.BROWSER_RESPONSE_TIMEOUT]);
            var timeout = TimeSpan.FromSeconds(timeoutInSec);
            var extractor = new Func<HttpListenerRequest, Result<OAuthAuthorizationCodeResponse, IBrowserError>>(httpRequest => ValidateAndExtractAuthorizationCodeResult(httpRequest, request.State));
            using (var httpListener = StartListenerUpdatingRedirectUri(request))
            using (var browserListener = new WebBrowserListener<OAuthAuthorizationCodeResponse>(httpListener, extractor, BrowserSuccessResponse, BrowserUnexpectedErrorResponse))
            {
                var authorizationCodeUrl = request.GetUrl();
                _browserStarter.StartBrowser(authorizationCodeUrl);
                return browserListener.WaitAndGetResult(timeout);
            }
        }

        private Result<OAuthAuthorizationCodeResponse, IBrowserError> ValidateAndExtractAuthorizationCodeResult(HttpListenerRequest request, string expectedState)
        {
            if (request.HttpMethod != "GET")
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new BrowserError
                {
                    BrowserMessage = BadRequestError("<br><b>Error</b>: Expected GET http method.</br>"),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_WRONG_METHOD, request.HttpMethod)
                });
            }

            if (string.IsNullOrEmpty(request.Url.Query))
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new BrowserError
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
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new BrowserError
                {
                    BrowserMessage = BadRequestError(errorMessage),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, $"Authorization code response has error '{error}' and description '{errorDescription}'")
                });
            }
            var authorizationCode = parameters.Get("code");
            var state = parameters.Get("state");
            if (string.IsNullOrEmpty(authorizationCode))
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new BrowserError
                {
                    BrowserMessage = BadRequestError("<br><b>Error</b>: Authorization code is required in the authorization code response</br>"),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, "Authorization code is required in the authorization code response")
                });
            }
            if (string.IsNullOrEmpty(state))
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new BrowserError
                {
                    BrowserMessage = BadRequestError("<br><b>Error</b>: State is required in the authorization code response</br>"),
                    Exception = new SnowflakeDbException(SFError.BROWSER_RESPONSE_ERROR, "State is required in the authorization code response")
                });
            }
            if (state != expectedState)
            {
                return Result<OAuthAuthorizationCodeResponse, IBrowserError>.CreateError(new BrowserError
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
