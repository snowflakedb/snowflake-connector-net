using Xunit;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class BasicAuthenticatorTests
    {
        [Theory]
        [InlineData("snowflake", true)]
        [InlineData("SNOWFLAKE", true)]
        [InlineData("username_password_mfa", false)]
        public void TestRecognizeBasicAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = BasicAuthenticator.IsBasicAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }
    }
}
