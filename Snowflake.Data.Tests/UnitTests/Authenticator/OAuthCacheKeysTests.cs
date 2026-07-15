using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Moq;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class OAuthCacheKeysTests
    {
        private const string Token = "abc";

        [SFTheory]
        [InlineData("testUser", true, true)]
        [InlineData("", true, false)]
        [InlineData(null, true, false)]
        [InlineData("testUser", false, false)]
        [InlineData("", false, false)]
        [InlineData(null, false, false)]
        public void TestCacheAvailableForAuthorizationCodeFlow(string user, bool clientStoreTemporaryCredentials, bool expectedIsAvailable)
        {
            // arrange
            var idpUrl = "https://idp.snowflakecomputing.com/oauth/token";
            var snowflakeHost = "snowflakecomputing.com";
            var role = "PUBLIC";
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow(idpUrl, snowflakeHost, user, role, clientStoreTemporaryCredentials, SnowflakeCredentialManagerFactory.GetCredentialManager);

            // act
            var isAvailable = cacheKeys.IsAvailable();

            // assert
            Assert.Equal(expectedIsAvailable, isAvailable);
        }

        [SFFact]
        public void TestCacheDisabledCacheIsNotAvailable()
        {
            // arrange
            var cacheKeys = OAuthCacheKeys.CreateForDisabledCache();

            // act
            var isAvailable = cacheKeys.IsAvailable();

            // assert
            Assert.False(isAvailable);
        }

        [SFFact]
        public void TestNoInteractionWithCacheWhenNotAvailable()
        {
            // arrange
            var credentialManager = new Mock<ISnowflakeCredentialManager>();
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow(null, null, null, null, false, () => credentialManager.Object);

            // act
            cacheKeys.GetAccessToken();
            cacheKeys.GetRefreshToken();
            cacheKeys.SaveAccessToken(Token);
            cacheKeys.SaveRefreshToken(Token);
            cacheKeys.RemoveAccessToken();
            cacheKeys.RemoveRefreshToken();

            // assert
            Assert.False(cacheKeys.IsAvailable());
            credentialManager.VerifyNoOtherCalls();
        }

        [SFFact]
        public void TestOperationsDontFailForDisabledCache()
        {
            // arrange
            var cacheKeys = OAuthCacheKeys.CreateForDisabledCache();

            // act/assert
            cacheKeys.GetAccessToken();
            cacheKeys.GetRefreshToken();
            cacheKeys.SaveAccessToken(Token);
            cacheKeys.SaveRefreshToken(Token);
            cacheKeys.RemoveAccessToken();
            cacheKeys.RemoveRefreshToken();
        }

        [SFFact]
        public void TestUseCacheForAccessToken()
        {
            // arrange
            var credentialManager = new SFCredentialManagerInMemoryImpl();
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow("https://idp.snowflakecomputing.com/oauth/token", "snowflakecomputing.com", "testUser", "PUBLIC", true, () => credentialManager);

            // act/assert
            Assert.True(cacheKeys.IsAvailable());
            Assert.Equal(string.Empty, cacheKeys.GetAccessToken());
            cacheKeys.SaveAccessToken(Token);
            Assert.Equal(Token, cacheKeys.GetAccessToken());
            cacheKeys.RemoveAccessToken();
            Assert.Equal(string.Empty, cacheKeys.GetAccessToken());
        }

        [SFFact]
        public void TestUseCacheForRefreshToken()
        {
            // arrange
            var credentialManager = new SFCredentialManagerInMemoryImpl();
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow("https://idp.snowflakecomputing.com/oauth/token", "snowflakecomputing.com", "testUser", "PUBLIC", true, () => credentialManager);

            // act/assert
            Assert.True(cacheKeys.IsAvailable());
            Assert.Equal(string.Empty, cacheKeys.GetRefreshToken());
            cacheKeys.SaveRefreshToken(Token);
            Assert.Equal(Token, cacheKeys.GetRefreshToken());
            cacheKeys.RemoveRefreshToken();
            Assert.Equal(string.Empty, cacheKeys.GetRefreshToken());
        }
    }
}
