using NUnit.Framework;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class OktaAuthenticatorTest
    {
        [Test]
        [TestCase("https://xxxxxx.okta.com", true)]
        [TestCase("https://xxxxxx.oktapreview.com", true)]
        [TestCase("https://vanity.url/snowflake/okta", true)]
        [TestCase("http://xxxxxx.okta.com", false)]
        [TestCase("https://xxxxxx.com", false)]
        [TestCase("username_password_mfa", false)]
        public void TestRecognizeOktaAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = OktaAuthenticator.IsOktaAuthenticator(authenticator);

            // assert
            Assert.AreEqual(expectedResult, result);
        }
    }
}
