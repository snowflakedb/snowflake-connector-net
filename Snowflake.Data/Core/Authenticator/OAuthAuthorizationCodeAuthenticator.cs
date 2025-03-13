using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator.Browser;
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

        private static readonly TimeSpan s_idpRestTimeout = TimeSpan.FromSeconds(120);

        private readonly ChallengeProvider _challengeProvider;
        private readonly WebBrowserStarter _browserStarter;

        public OAuthAuthorizationCodeAuthenticator(SFSession session) : base(session, OAuthAuthenticator.AUTH_NAME)
        {
            _challengeProvider = new ChallengeProvider();
            _browserStarter = WebBrowserStarter.Instance;
        }

        internal OAuthAuthorizationCodeAuthenticator(SFSession session, ChallengeProvider challengeProvider, WebBrowserStarter browserStarter) : this(session)
        {
            _challengeProvider = challengeProvider;
            _browserStarter = browserStarter;
        }

        public static bool IsOAuthAuthorizationCodeAuthenticator(string authenticator) =>
            AuthName.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        public async Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            var accessTokenRequest = RunFlowToAccessTokenRequest();
            var restRequester = (RestRequester) session.restRequester;
            using (var accessTokenHttpRequest = accessTokenRequest.CreateHttpRequest())
            {
                var restRequest = new RestRequestWrapper(accessTokenHttpRequest, s_idpRestTimeout);
                var accessTokenResponse = await restRequester.PostAsync<OAuthAccessTokenResponse>(restRequest, cancellationToken).ConfigureAwait(false);
                HandleAccessTokenResponse(accessTokenResponse);
            }
            await base.LoginAsync(cancellationToken).ConfigureAwait(false);
        }

        public void Authenticate()
        {
            var accessTokenRequest = RunFlowToAccessTokenRequest();
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
                HandleAccessTokenResponse(accessTokenResponse);
            }
            base.Login();
        }

        private Exception UnpackAggregateException(Exception exception) =>
            exception is AggregateException ? ((AggregateException)exception).InnerException : exception;

        private OAuthAccessTokenRequest RunFlowToAccessTokenRequest()
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
            var authorizationCodeResult = ExecuteAuthorizationCodeRequest(authorizationCodeRequest);
            return new OAuthAccessTokenRequest
            {
                TokenEndpoint = GetTokenEndpoint(),
                AuthorizationCode = authorizationCodeResult.AuthorizationCode,
                AuthorizationScope = authorizationCodeRequest.AuthorizationScope,
                ClientId = authorizationCodeRequest.ClientId,
                ClientSecret = RequiredProperty(SFSessionProperty.OAUTHCLIENTSECRET),
                CodeVerifier = codeVerifier.Value,
                RedirectUri = authorizationCodeRequest.RedirectUri
            };
        }

        private void HandleAccessTokenResponse(OAuthAccessTokenResponse accessTokenResponse)
        {
            var utcNow = DateTime.UtcNow;
            accessTokenResponse.Validate();
            var accessToken = accessTokenResponse.GetAccessToken(utcNow);
            var refreshToken = accessTokenResponse.GetRefreshToken(utcNow);
            session._accessToken = accessToken;
        }

        private OAuthAuthorizationCodeResponse ExecuteAuthorizationCodeRequest(OAuthAuthorizationCodeRequest request)
        {
            var timeoutInSec = int.Parse(session.properties[SFSessionProperty.BROWSER_RESPONSE_TIMEOUT]);
            var timeout = TimeSpan.FromSeconds(timeoutInSec);
            var extractor = new Func<HttpListenerRequest, Result<OAuthAuthorizationCodeResponse, IBrowserError>>(httpRequest => ValidateAndExtractAuthorizationCodeResult(httpRequest, request.State));
            var redirectUri = request.RedirectUri.EndsWith("/") ? request.RedirectUri : request.RedirectUri + "/"; // TODO: handling of default redirect_uri will be added later
            using (var httpListener = StartHttpListener(redirectUri))
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
            data.Token = session.GetAccessToken(DateTime.UtcNow);
            if (string.IsNullOrEmpty(data.Token))
            {
                throw new Exception("No valid access token to use");
            }
            SetSecondaryAuthenticationData(ref data);
        }

        private HttpListener StartHttpListener(string url)
        {
            var listener = new HttpListener();
            listener.Prefixes.Add(url);
            listener.Start();
            return listener;
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
    }
}
