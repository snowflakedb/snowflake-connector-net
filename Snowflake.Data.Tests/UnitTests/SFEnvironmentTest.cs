using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFEnvironmentTest
    {
        [Test]
        public void TestRuntimeExtraction()
        {
            // Arrange
            string expectedRuntime;

#if NETFRAMEWORK
            expectedRuntime = ".NETFramework";
#else
            expectedRuntime = ".NET";
#endif

            // Act
            var actualRuntime = SFEnvironment.ExtractRuntime();

            // Assert
            Assert.AreEqual(expectedRuntime, actualRuntime);
        }
    }
}
