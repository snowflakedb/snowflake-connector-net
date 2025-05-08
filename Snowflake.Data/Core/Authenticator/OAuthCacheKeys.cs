using System;
using Snowflake.Data.Client;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    internal class OAuthCacheKeys
    {
        private readonly string _accessTokenKey;
        private readonly string _refreshTokenKey;
        private readonly Func<ISnowflakeCredentialManager> _credentialManagerProvider;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OAuthCacheKeys>();

        public static OAuthCacheKeys CreateForAuthorizationCodeFlow(string host, string user, bool clientStoreTemporaryCredentials, Func<ISnowflakeCredentialManager> credentialManagerProvider)
        {
            string accessTokenKey = string.Empty;
            string refreshTokenKey = string.Empty;
            if (IsCacheAvailableForAuthorizationCodeFlow(user, clientStoreTemporaryCredentials, true))
            {
                accessTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.OAuthAccessToken);
                refreshTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.OAuthRefreshToken);
            }
            return new OAuthCacheKeys(accessTokenKey, refreshTokenKey, credentialManagerProvider);
        }

        public static OAuthCacheKeys CreateForDisabledCache()
        {
            return new OAuthCacheKeys(string.Empty, string.Empty, null);
        }

        public static bool IsCacheAvailableForAuthorizationCodeFlow(string user, bool clientStoreTemporaryCredentials, bool logReasons)
        {
            if (string.IsNullOrEmpty(user))
            {
                if (logReasons)
                    s_logger.Debug("Cache in OAuth flow is not used because user is not defined");
                return false;
            }

            if (!clientStoreTemporaryCredentials)
            {
                if (logReasons)
                    s_logger.Debug("Cache in OAuth flow is not used because clientStoreTemporaryCredentials is false");
                return false;
            }
            return true;
        }

        private OAuthCacheKeys(string accessTokenKey, string refreshTokenKey, Func<ISnowflakeCredentialManager> credentialManagerProvider)
        {
            _accessTokenKey = accessTokenKey;
            _refreshTokenKey = refreshTokenKey;
            _credentialManagerProvider = credentialManagerProvider;
        }

        public bool IsAvailable() => !string.IsNullOrEmpty(_accessTokenKey);

        public string GetAccessToken() => GetToken(_accessTokenKey);

        public string GetRefreshToken() => GetToken(_refreshTokenKey);

        public void SaveAccessToken(string accessToken)
        {
            if (IsAvailable())
            {
                _credentialManagerProvider.Invoke().SaveCredentials(_accessTokenKey, accessToken);
            }
        }

        public void SaveRefreshToken(string refreshToken)
        {
            if (IsAvailable())
            {
                _credentialManagerProvider.Invoke().SaveCredentials(_refreshTokenKey, refreshToken);
            }
        }

        public void RemoveAccessToken()
        {
            if (IsAvailable())
            {
                _credentialManagerProvider.Invoke().RemoveCredentials(_accessTokenKey);
            }
        }

        public void RemoveRefreshToken()
        {
            if (IsAvailable())
            {
                _credentialManagerProvider.Invoke().RemoveCredentials(_refreshTokenKey);
            }
        }

        private string GetToken(string key) =>
            string.IsNullOrEmpty(key) ? string.Empty : _credentialManagerProvider.Invoke().GetCredentials(key);
    }
}
