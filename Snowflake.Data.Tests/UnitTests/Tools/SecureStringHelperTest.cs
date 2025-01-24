using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture]
    public class SecureStringHelperTest
    {
        [Test]
        public void TestConvertPassword()
        {
            // arrange
            var passwordText = "testPassword";

            // act
            var securePassword = SecureStringHelper.Encode(passwordText);
            var decodedPassword = SecureStringHelper.Decode(securePassword);

            // assert
            Assert.AreEqual(passwordText, decodedPassword);
        }
    }
}
