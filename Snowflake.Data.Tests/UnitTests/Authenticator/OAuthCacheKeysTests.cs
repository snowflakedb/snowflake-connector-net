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
        [TestCase( "testUser", true)]
        [TestCase("", false)]
        [TestCase( null, false)]
        public void TestCacheAvailable(string user, bool expectedIsAvailable)
        {
            // arrange
            var host = "snowflakecomputing.com";
            var cacheKeys = new OAuthCacheKeys(host, user, SnowflakeCredentialManagerFactory.GetCredentialManager);

            // act
            var isAvailable = cacheKeys.IsAvailable();

            // assert
            Assert.AreEqual(expectedIsAvailable, isAvailable);
        }

        [Test]
        public void TestNoInteractionWithCacheWhenNotAvailable()
        {
            // arrange
            var credentialManager = new Mock<ISnowflakeCredentialManager>();
            var cacheKeys = new OAuthCacheKeys(null, null, () => credentialManager.Object);

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
        public void TestUseCacheForAccessToken()
        {
            // arrange
            var credentialManager = new SFCredentialManagerInMemoryImpl();
            var cacheKeys = new OAuthCacheKeys("snowflakecomputing.com", "testUser", () => credentialManager);

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
            var cacheKeys = new OAuthCacheKeys("snowflakecomputing.com", "testUser", () => credentialManager);

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
