using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class SecureStringHelperTest
    {
        [SFFact]
        public void TestConvertPassword()
        {
            // arrange
            var passwordText = "testPassword";

            // act
            var securePassword = SecureStringHelper.Encode(passwordText);
            var decodedPassword = SecureStringHelper.Decode(securePassword);

            // assert
            Assert.Equal(passwordText, decodedPassword);
        }
    }
}
