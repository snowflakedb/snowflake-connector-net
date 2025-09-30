using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Moq;
using Snowflake.Data.Core.CredentialManager.Infrastructure;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class OAuthCacheKeysTests
    {
        private const string Token = "abc";

        [Test]
        [TestCase("testUser", true, true)]
        [TestCase("", true, false)]
        [TestCase(null, true, false)]
        [TestCase("testUser", false, false)]
        [TestCase("", false, false)]
        [TestCase(null, false, false)]
        public void TestCacheAvailableForAuthorizationCodeFlow(string user, bool clientStoreTemporaryCredentials, bool expectedIsAvailable)
        {
            // arrange
            var host = "snowflakecomputing.com";
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow(host, user, clientStoreTemporaryCredentials, SnowflakeCredentialManagerFactory.GetCredentialManager);

            // act
            var isAvailable = cacheKeys.IsAvailable();

            // assert
            Assert.AreEqual(expectedIsAvailable, isAvailable);
        }

        [Test]
        public void TestCacheDisabledCacheIsNotAvailable()
        {
            // arrange
            var cacheKeys = OAuthCacheKeys.CreateForDisabledCache();

            // act
            var isAvailable = cacheKeys.IsAvailable();

            // assert
            Assert.AreEqual(false, isAvailable);
        }

        [Test]
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
            Assert.IsFalse(cacheKeys.IsAvailable());
            credentialManager.VerifyNoOtherCalls();
        }

        [Test]
        public void TestOperationsDontFailForDisabledCache()
        {
            // arrange
            var cacheKeys = OAuthCacheKeys.CreateForDisabledCache();

            // act/assert
            Assert.DoesNotThrow(() =>
            {
                cacheKeys.GetAccessToken();
                cacheKeys.GetRefreshToken();
                cacheKeys.SaveAccessToken(Token);
                cacheKeys.SaveRefreshToken(Token);
                cacheKeys.RemoveAccessToken();
                cacheKeys.RemoveRefreshToken();
            });
        }

        [Test]
        public void TestUseCacheForAccessToken()
        {
            // arrange
            var credentialManager = new SFCredentialManagerInMemoryImpl();
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow("snowflakecomputing.com", "testUser", true, () => credentialManager);

            // act/assert
            Assert.IsTrue(cacheKeys.IsAvailable());
            Assert.AreEqual(string.Empty, cacheKeys.GetAccessToken());
            cacheKeys.SaveAccessToken(Token);
            Assert.AreEqual(Token, cacheKeys.GetAccessToken());
            cacheKeys.RemoveAccessToken();
            Assert.AreEqual(string.Empty, cacheKeys.GetAccessToken());
        }

        [Test]
        public void TestUseCacheForRefreshToken()
        {
            // arrange
            var credentialManager = new SFCredentialManagerInMemoryImpl();
            var cacheKeys = OAuthCacheKeys.CreateForAuthorizationCodeFlow("snowflakecomputing.com", "testUser", true, () => credentialManager);

            // act/assert
            Assert.IsTrue(cacheKeys.IsAvailable());
            Assert.AreEqual(string.Empty, cacheKeys.GetRefreshToken());
            cacheKeys.SaveRefreshToken(Token);
            Assert.AreEqual(Token, cacheKeys.GetRefreshToken());
            cacheKeys.RemoveRefreshToken();
            Assert.AreEqual(string.Empty, cacheKeys.GetRefreshToken());
        }
    }
}
