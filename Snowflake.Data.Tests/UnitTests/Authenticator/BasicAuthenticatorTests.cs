using NUnit.Framework;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class BasicAuthenticatorTests
    {
        [Test]
        [TestCase("snowflake", true)]
        [TestCase("SNOWFLAKE", true)]
        [TestCase("username_password_mfa", false)]
        public void TestRecognizeBasicAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = BasicAuthenticator.IsBasicAuthenticator(authenticator);

            // assert
            Assert.AreEqual(expectedResult, result);
        }
    }
}
