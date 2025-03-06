using NUnit.Framework;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class MFACacheAuthenticatorTest
    {
        [Test]
        [TestCase("username_password_mfa", true)]
        [TestCase("USERNAME_PASSWORD_MFA", true)]
        [TestCase("snowflake", false)]
        public void TestRecognizeMfaAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = MFACacheAuthenticator.IsMfaCacheAuthenticator(authenticator);

            // assert
            Assert.AreEqual(expectedResult, result);
        }
    }
}
