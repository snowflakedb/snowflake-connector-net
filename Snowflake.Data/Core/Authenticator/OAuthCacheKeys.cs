using Snowflake.Data.Client;
using Snowflake.Data.Core.CredentialManager;

namespace Snowflake.Data.Core.Authenticator
{
    internal class OAuthCacheKeys
    {
        public string AccessTokenKey { get; }
        public string RefreshTokenKey { get; }

        public OAuthCacheKeys(string host, string user)
        {
            if (string.IsNullOrEmpty(user) || string.IsNullOrEmpty(host))
            {
                AccessTokenKey = string.Empty;
                RefreshTokenKey = string.Empty;
                return;
            }
            AccessTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.OAuthAccessToken);
            RefreshTokenKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.OAuthRefreshToken);
        }

        public bool IsAvailable() => !string.IsNullOrEmpty(AccessTokenKey);

        public string GetAccessToken() => GetToken(AccessTokenKey);

        public string GetRefreshToken() => GetToken(RefreshTokenKey);

        public void SaveAccessToken(string accessToken)
        {
            if (IsAvailable())
            {
                SnowflakeCredentialManagerFactory.GetCredentialManager().SaveCredentials(AccessTokenKey, accessToken);
            }
        }

        public void SaveRefreshToken(string refreshToken)
        {
            if (IsAvailable())
            {
                SnowflakeCredentialManagerFactory.GetCredentialManager().SaveCredentials(RefreshTokenKey, refreshToken);
            }
        }

        public void RemoveAccessToken()
        {
            if (IsAvailable())
            {
                SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(AccessTokenKey);
            }
        }

        public void RemoveRefreshToken()
        {
            if (IsAvailable())
            {
                SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(RefreshTokenKey);
            }
        }

        private string GetToken(string key) =>
            string.IsNullOrEmpty(key) ? string.Empty : SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key);
    }
}
