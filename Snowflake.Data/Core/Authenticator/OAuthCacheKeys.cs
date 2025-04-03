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

        public OAuthCacheKeys(string host, string user, bool clientStoreTemporaryCredentials, Func<ISnowflakeCredentialManager> credentialManagerProvider)
        {
            _credentialManagerProvider = credentialManagerProvider;
            if (string.IsNullOrEmpty(user))
            {
                s_logger.Debug("Cache in OAuth flow is not used because user is not defined");
                _accessTokenKey = string.Empty;
                _refreshTokenKey = string.Empty;
                return;
            }

            if (!clientStoreTemporaryCredentials)
            {
                s_logger.Debug("Cache in OAuth flow is not used because clientStoreTemporaryCredentials is false");
                _accessTokenKey = string.Empty;
                _refreshTokenKey = string.Empty;
                return;
            }
            _accessTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.OAuthAccessToken);
            _refreshTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.OAuthRefreshToken);
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
