using Xunit;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class OAuthAuthenticatorTest
    {
        [SFTheory]
        [InlineData("oauth", true)]
        [InlineData("OAUTH", true)]
        [InlineData("username_password_mfa", false)]
        public void TestRecognizeOAuthAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = OAuthAuthenticator.IsOAuthAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }
    }
}
