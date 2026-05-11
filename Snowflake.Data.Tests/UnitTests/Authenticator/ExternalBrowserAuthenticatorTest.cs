using Xunit;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class ExternalBrowserAuthenticatorTest
    {
        [Theory]
        [InlineData("externalbrowser", true)]
        [InlineData("EXTERNALBROWSER", true)]
        [InlineData("username_password_mfa", false)]
        public void TestRecognizeExternalBrowserAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = ExternalBrowserAuthenticator.IsExternalBrowserAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }
    }
}
