using NUnit.Framework;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class KeyPairAuthenticatorTest
    {
        [Test]
        [TestCase("snowflake_jwt", true)]
        [TestCase("SNOWFLAKE_JWT", true)]
        [TestCase("username_password_mfa", false)]
        public void TestRecognizeKeyPairAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = KeyPairAuthenticator.IsKeyPairAuthenticator(authenticator);

            // assert
            Assert.AreEqual(expectedResult, result);
        }
    }
}
