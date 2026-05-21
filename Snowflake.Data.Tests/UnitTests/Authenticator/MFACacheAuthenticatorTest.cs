using Xunit;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{

    public class MFACacheAuthenticatorTest
    {
        [SFFact]
        [InlineData("username_password_mfa", true)]
        [InlineData("USERNAME_PASSWORD_MFA", true)]
        [InlineData("snowflake", false)]
        public void TestRecognizeMfaAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = MFACacheAuthenticator.IsMfaCacheAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }
    }
}
