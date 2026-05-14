using Xunit;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class ExternalBrowserAuthenticatorTest
    {
        [SFTheory]
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
