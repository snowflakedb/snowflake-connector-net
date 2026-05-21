using Xunit;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{

    public class KeyPairAuthenticatorTest
    {
        [SFTheory]
        [InlineData("snowflake_jwt", true)]
        [InlineData("SNOWFLAKE_JWT", true)]
        [InlineData("username_password_mfa", false)]
        public void TestRecognizeKeyPairAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = KeyPairAuthenticator.IsKeyPairAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }
    }
}
