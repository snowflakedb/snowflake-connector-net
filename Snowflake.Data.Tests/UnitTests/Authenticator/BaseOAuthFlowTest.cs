using System.IO;
using NUnit.Framework;
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
        protected const string TokenHost = "localhost";
        protected const string ClientSecret = "123";

        internal void AssertSessionSuccessfullyCreated(SFSession session)
        {
            Assert.AreEqual(SessionId, session.sessionId);
            Assert.AreEqual(MasterToken, session.masterToken);
            Assert.AreEqual(SessionToken, session.sessionToken);
        }

        protected int InMemoryCacheCount()
        {
            var credentialManager = (SFCredentialManagerInMemoryImpl)SnowflakeCredentialManagerFactory.GetCredentialManager();
            return credentialManager.GetCount();
        }

        internal string ExtractTokenFromCache(TokenType tokenType)
        {
            var cacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(TokenHost, User, tokenType);
            return SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(cacheKey);
        }

        internal void SaveTokenToCache(TokenType tokenType, string token)
        {
            var cacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(TokenHost, User, tokenType);
            SnowflakeCredentialManagerFactory.GetCredentialManager().SaveCredentials(cacheKey, token);
        }

        internal void RemoveTokenFromCache(TokenType tokenType)
        {
            var cacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(TokenHost, User, tokenType);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(cacheKey);
        }
    }
}
