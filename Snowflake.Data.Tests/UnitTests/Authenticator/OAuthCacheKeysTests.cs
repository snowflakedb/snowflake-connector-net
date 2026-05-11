using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Moq;
using Snowflake.Data.Core.CredentialManager.Infrastructure;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class OAuthCacheKeysTests
    {
        private const string Token = "abc";

        [Theory]
        [InlineData("testUser", true, true)]
        [InlineData("", true, false)]
        [InlineData(null, true, false)]
        [InlineData("testUser", false, false)]
        [InlineData("", false, false)]
        [InlineData(null, false, false)]
        public void TestCacheAvailableForAuthorizationCodeFlow(string user, bool clientStoreTemporaryCredentials, bool expectedIsAvailable)
        {
            // arrange
            var host = "snowflakecomputing.com";
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow(host, user, clientStoreTemporaryCredentials, SnowflakeCredentialManagerFactory.GetCredentialManager);

            // act
            var isAvailable = cacheKeys.IsAvailable();

            // assert
            Assert.Equal(expectedIsAvailable, isAvailable);
        }

        [Fact]
        public void TestCacheDisabledCacheIsNotAvailable()
        {
            // arrange
            var cacheKeys = OAuthCacheKeys.CreateForDisabledCache();

            // act
            var isAvailable = cacheKeys.IsAvailable();

            // assert
            Assert.Equal(false, isAvailable);
        }

        [Fact]
        public void TestNoInteractionWithCacheWhenNotAvailable()
        {
            // arrange
            var credentialManager = new Mock<ISnowflakeCredentialManager>();
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow(null, null, false, () => credentialManager.Object);

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

        [Fact]
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

        [Fact]
        public void TestUseCacheForAccessToken()
        {
            // arrange
            var credentialManager = new SFCredentialManagerInMemoryImpl();
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow("snowflakecomputing.com", "testUser", true, () => credentialManager);

            // act/assert
            Assert.True(cacheKeys.IsAvailable());
            Assert.Equal(string.Empty, cacheKeys.GetAccessToken());
            cacheKeys.SaveAccessToken(Token);
            Assert.Equal(Token, cacheKeys.GetAccessToken());
            cacheKeys.RemoveAccessToken();
            Assert.Equal(string.Empty, cacheKeys.GetAccessToken());
        }

        [Fact]
        public void TestUseCacheForRefreshToken()
        {
            // arrange
            var credentialManager = new SFCredentialManagerInMemoryImpl();
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow("snowflakecomputing.com", "testUser", true, () => credentialManager);

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
