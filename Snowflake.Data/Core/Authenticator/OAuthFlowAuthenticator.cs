using System;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    internal abstract class OAuthFlowAuthenticator : BaseAuthenticator
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OAuthFlowAuthenticator>();
        private static readonly TimeSpan s_idpRestTimeout = TimeSpan.FromSeconds(120);

        internal SecureString AccessToken { get; private set; } = null;

        protected OAuthFlowAuthenticator(SFSession session, string authName) : base(session, authName)
        {
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
                var renewAccessTokenRequest = GetRenewAccessTokenRequest(exception, cacheKeys);
                if (renewAccessTokenRequest != null)
                {
                    AccessToken = null;
                    GetAccessToken(renewAccessTokenRequest, cacheKeys);
                }
                if (renewAccessTokenRequest != null && AccessToken != null)
                {
                    Login();
                    return;
                }
                s_logger.Error($"Could not renew access token in {GetAuthenticatorName()} flow, returning the original error: {exception.Message}");
                throw;
            }
        }

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
                var renewAccessTokenRequest = GetRenewAccessTokenRequest(exception, cacheKeys);
                if (renewAccessTokenRequest != null)
                {
                    AccessToken = null;
                    await GetAccessTokenAsync(renewAccessTokenRequest, cacheKeys, cancellationToken).ConfigureAwait(false);
                }
                if (renewAccessTokenRequest != null && AccessToken != null)
                {
                    await LoginAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }
                s_logger.Error($"Could not renew access token in {GetAuthenticatorName()} flow, returning the original error: {exception.Message}");
                throw;
            }
        }

        protected abstract OAuthAccessTokenRequest RunFlowToAccessTokenRequest();

        protected abstract BaseOAuthAccessTokenRequest GetRenewAccessTokenRequest(SnowflakeDbException exception, OAuthCacheKeys cacheKeys);

        protected abstract string GetAuthenticatorName();

        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.OAuthType = GetAuthenticatorName();
            data.Token = (AccessToken == null ? null : SecureStringHelper.Decode(AccessToken));
            if (string.IsNullOrEmpty(data.Token))
            {
                var errorMessage = $"No valid access token is available to use for {GetAuthenticatorName()} authentication";
                s_logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
            data.loginName = session.properties[SFSessionProperty.USER];
            SetSecondaryAuthenticationData(ref data);
        }

        private void GetAccessToken(
            BaseOAuthAccessTokenRequest accessTokenRequest,
            OAuthCacheKeys cacheKeys)
        {
            var authName = GetAuthenticatorName();
            s_logger.Debug($"Getting access token for {authName} authentication");
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
                    s_logger.Error($"Failed to get access token for {authName} authentication: {realException.Message}");
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
            var authName = GetAuthenticatorName();
            s_logger.Debug($"Getting access token (async) for {authName} authentication");
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
                    s_logger.Error($"Failed to get access token (async) for {authName} authentication: {realException.Message}");
                    throw new SnowflakeDbException(SFError.OAUTH_TOKEN_REQUEST_ERROR, realException.Message);
                }
                HandleAccessTokenResponse(accessTokenResponse, cacheKeys);
            }
        }

        protected bool IsAccessTokenExpiredOrInvalid(SnowflakeDbException exception) =>
            OAuthTokenErrors.IsAccessTokenExpired(exception.ErrorCode) || OAuthTokenErrors.IsAccessTokenInvalid(exception.ErrorCode);

        private Exception UnpackAggregateException(Exception exception) =>
            exception is AggregateException ? ((AggregateException)exception).InnerException : exception;

        private void HandleAccessTokenResponse(OAuthAccessTokenResponse accessTokenResponse, OAuthCacheKeys cacheKeys)
        {
            accessTokenResponse.Validate();
            var accessToken = accessTokenResponse.AccessToken;
            var refreshToken = accessTokenResponse.RefreshToken;
            cacheKeys.SaveAccessToken(accessToken);
            if (string.IsNullOrEmpty(refreshToken))
            {
                s_logger.Debug("Access token without refresh token received from Identity Provider");
                cacheKeys.RemoveRefreshToken();
            }
            else
            {
                s_logger.Debug("Access token and refresh token received from Identity Provider");
                cacheKeys.SaveRefreshToken(refreshToken);
            }
            AccessToken = SecureStringHelper.Encode(accessToken);
        }

        private OAuthCacheKeys GetOAuthCacheKeys()
        {
            var host = new Uri(GetTokenEndpoint()).Host;
            var user = session.properties[SFSessionProperty.USER];
            return new OAuthCacheKeys(host, user, SnowflakeCredentialManagerFactory.GetCredentialManager);
        }

        protected OAuthRefreshAccessTokenRequest BuildRefreshTokenRequest(OAuthCacheKeys cacheKeys)
        {
            var refreshToken = cacheKeys.GetRefreshToken();
            if (string.IsNullOrEmpty(refreshToken))
            {
                s_logger.Debug($"The access token cannot be refreshed because there is no refresh token saved for this {GetAuthenticatorName()} authentication");
                return null;
            }
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

        protected string GetAuthorizationScope()
        {
            var scope = ExtractPropertyOrEmptyString(SFSessionProperty.OAUTHSCOPE);
            if (!string.IsNullOrEmpty(scope))
                return scope;
            var role = RequiredProperty(SFSessionProperty.ROLE);
            return OAuthFlowConfig.DefaultScopePrefixBeforeRole + role;
        }

        protected string GetTokenEndpoint()
        {
            var externalTokenUrl = ExtractPropertyOrEmptyString(SFSessionProperty.OAUTHTOKENREQUESTURL);
            if (!string.IsNullOrEmpty(externalTokenUrl))
                return externalTokenUrl;
            return DefaultSnowflakeEndpoint(OAuthFlowConfig.SnowflakeTokenUrl);
        }

        protected string DefaultSnowflakeEndpoint(string relativeUrl)
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

        protected string RequiredProperty(SFSessionProperty property)
        {
            var value = ExtractPropertyOrEmptyString(property);
            if (string.IsNullOrEmpty(value))
            {
                throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, $"Property {property.ToString()} is required for OAuth authorization code flow");
            }
            return value;
        }
        protected string ExtractPropertyOrEmptyString(SFSessionProperty property) => ExtractPropertyOrDefault(property, string.Empty);

        private string ExtractPropertyOrDefault(SFSessionProperty property, string defaultValue)
        {
            if (session.properties.TryGetValue(property, out string value) && !string.IsNullOrEmpty(value))
                return value;
            return defaultValue;
        }
    }
}
