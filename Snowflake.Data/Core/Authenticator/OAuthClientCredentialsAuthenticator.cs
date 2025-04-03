using System;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    internal class OAuthClientCredentialsAuthenticator: OAuthFlowAuthenticator, IAuthenticator
    {
        public const string AuthName = "oauth_client_credentials";
        private const string TokenRequestGrantType = "client_credentials";

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OAuthClientCredentialsAuthenticator>();

        public static bool IsOAuthClientCredentialsAuthenticator(string authenticator) =>
            AuthName.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        public OAuthClientCredentialsAuthenticator(SFSession session) : base(session, OAuthAuthenticator.AUTH_NAME)
        {
        }

        protected override string GetAuthenticatorName() => AuthName;

        protected override OAuthAccessTokenRequest RunFlowToAccessTokenRequest()
        {
            return new OAuthAccessTokenRequest
            {
                TokenEndpoint = GetTokenEndpoint(),
                GrantType = TokenRequestGrantType,
                ClientId = RequiredProperty(SFSessionProperty.OAUTHCLIENTID),
                ClientSecret = RequiredProperty(SFSessionProperty.OAUTHCLIENTSECRET),
                AuthorizationScope = GetAuthorizationScope()
            };
        }

        protected override BaseOAuthAccessTokenRequest GetRenewAccessTokenRequest(SnowflakeDbException exception, OAuthCacheKeys cacheKeys)
        {
            if (!IsAccessTokenExpiredOrInvalid(exception))
            {
                s_logger.Debug($"Exception code returned for {AuthName} authentication does not indicate expired or invalid token so the the authentication flow is failing");
                return null;
            }
            if (cacheKeys.IsAvailable())
            {
                var refreshTokenRequest = BuildRefreshTokenRequest(cacheKeys);
                if (refreshTokenRequest != null)
                {
                    s_logger.Debug("Refresh token is going to be used to refresh the access token");
                    return refreshTokenRequest;
                }
            }
            else
            {
                s_logger.Debug($"Cache in this {AuthName} authentication is disabled so there won't be any attempts to use a refresh token");
            }
            s_logger.Debug($"The {AuthName} flow is going to be replayed because using a refresh token was not possible");
            return RunFlowToAccessTokenRequest();
        }
    }
}
