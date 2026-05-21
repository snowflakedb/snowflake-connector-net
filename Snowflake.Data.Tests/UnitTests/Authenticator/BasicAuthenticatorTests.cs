using Xunit;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{

    public class BasicAuthenticatorTests
    {
        [SFFact]
        [TestCase("snowflake", true)]
        [TestCase("SNOWFLAKE", true)]
        [TestCase("username_password_mfa", false)]
        public void TestRecognizeBasicAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = BasicAuthenticator.IsBasicAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }
    }
}
