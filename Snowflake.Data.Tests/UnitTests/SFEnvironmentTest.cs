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
            string expectedVersion;

#if NETFRAMEWORK
            expectedRuntime = ".NETFramework";
            expectedVersion = "4.8.4724.0";
#else
            expectedRuntime = ".NET";
            expectedVersion = "8.0.2";
#endif

            // Act
            var actualRuntime = SFEnvironment.ExtractRuntime();
            var actualVersion = SFEnvironment.ExtractVersion();

            // Assert
            Assert.AreEqual(expectedRuntime, actualRuntime);
            Assert.AreEqual(expectedVersion, actualVersion);
        }
    }
}
