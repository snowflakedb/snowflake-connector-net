using NUnit.Framework;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class OAuthAuthenticatorTest
    {
        [Test]
        [TestCase("oauth", true)]
        [TestCase("OAUTH", true)]
        [TestCase("username_password_mfa", false)]
        public void TestRecognizeOAuthAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = OAuthAuthenticator.IsOAuthAuthenticator(authenticator);

            // assert
            Assert.AreEqual(expectedResult, result);
        }
    }
}
