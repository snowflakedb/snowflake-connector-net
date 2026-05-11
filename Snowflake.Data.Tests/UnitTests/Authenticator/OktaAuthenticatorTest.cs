using Xunit;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class OktaAuthenticatorTest
    {
        [Theory]
        [InlineData("https://xxxxxx.okta.com", true)]
        [InlineData("https://xxxxxx.oktapreview.com", true)]
        [InlineData("https://vanity.url/snowflake/okta", true)]
        [InlineData("http://xxxxxx.okta.com", false)]
        [InlineData("https://xxxxxx.com", false)]
        [InlineData("username_password_mfa", false)]
        public void TestRecognizeOktaAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = OktaAuthenticator.IsOktaAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }
    }
}
