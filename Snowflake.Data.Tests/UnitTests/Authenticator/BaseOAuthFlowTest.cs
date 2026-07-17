using System.IO;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class BaseOAuthFlowTest
    {
        protected static readonly string s_oauthMappingPath = Path.Combine("wiremock", "OAuth");
        protected static readonly string s_oauthSnowflakeLoginSuccessMappingPath = Path.Combine(s_oauthMappingPath, "snowflake_successful_login.json");
        protected static readonly string s_oauthSnowflakeLoginInvalidTokenMappingPath = Path.Combine(s_oauthMappingPath, "snowflake_invalid_token_login.json");
        protected static readonly string s_oauthAuthorizationCodeMappingPath = Path.Combine(s_oauthMappingPath, "AuthorizationCode");
        protected static readonly string s_refreshTokenMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "refresh_token.json");
        protected static readonly string s_externalTokenRequestUrl = $"http://localhost:{WiremockRunner.DefaultHttpPort}/oauth/token-request";

        protected const string MasterToken = "masterToken123";
        protected const string SessionToken = "sessionToken123";
        protected const string AccessToken = "access-token-123";
        protected const string RefreshToken = "refresh-token-123";
        protected const string NewAccessToken = "new-access-token-123";
        protected const string NewRefreshToken = "new-refresh-token-123";
        protected const string InvalidAccessToken = "invalid-access-token-123";
        protected const string SessionId = "1234567890";
        protected const string User = "testUser";
        protected const string AuthorizationScope = "session:role:ANALYST";
        protected const string Role = "ANALYST";
        // Must mirror the `host` set in the flow's connection string (WiremockRunner.Host), because the
        // v2 cache key's `snowflake` dimension is taken from the HOST session property, not the IdP URL.
        protected const string SnowflakeHost = WiremockRunner.Host;
        protected const string ClientSecret = "123";

        internal void AssertSessionSuccessfullyCreated(SFSession session)
        {
            Assert.Equal(SessionId, session.sessionId);
            Assert.Equal(MasterToken, session.masterToken);
            Assert.Equal(SessionToken, session.sessionToken);
        }

        protected int InMemoryCacheCount()
        {
            var credentialManager = (SFCredentialManagerInMemoryImpl)SnowflakeCredentialManagerFactory.GetCredentialManager();
            return credentialManager.GetCount();
        }

        internal string ExtractTokenFromCache(TokenType tokenType)
        {
            var cacheKey = BuildOAuthCacheKey(tokenType);
            return SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(cacheKey);
        }

        internal void SaveTokenToCache(TokenType tokenType, string token)
        {
            var cacheKey = BuildOAuthCacheKey(tokenType);
            SnowflakeCredentialManagerFactory.GetCredentialManager().SaveCredentials(cacheKey, token);
        }

        internal void RemoveTokenFromCache(TokenType tokenType)
        {
            var cacheKey = BuildOAuthCacheKey(tokenType);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(cacheKey);
        }

        private string BuildOAuthCacheKey(TokenType tokenType) =>
            SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                tokenType: tokenType.GetAttribute<StringAttr>().value,
                idp: s_externalTokenRequestUrl,
                snowflake: SnowflakeHost,
                username: User,
                role: Role
            ));
    }
}
