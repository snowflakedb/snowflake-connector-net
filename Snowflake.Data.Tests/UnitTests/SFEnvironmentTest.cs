using NUnit.Framework;
using Snowflake.Data.Core;
using System.Linq;
using System.Reflection;
using System.Runtime.Versioning;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFEnvironmentTest
    {
        [Test]
        public void TestRuntimeExtraction()
        {
            // Arrange
            var targetFrameworkAttribute = Assembly.GetExecutingAssembly().GetCustomAttributes(true).ToList().OfType<TargetFrameworkAttribute>().First();
            var targetFramework = targetFrameworkAttribute.FrameworkName.Split(',');
            var expectedRuntime = targetFramework[0].Replace("CoreApp", "");
            var expectedVersion = targetFramework[1].Substring(targetFramework[1].IndexOf("=v") + 2);

            if (expectedRuntime.Contains("Framework"))
            {
                expectedVersion = expectedVersion.Replace(".", "");
            }

            // Act
            var actualRuntime = SFEnvironment.ExtractRuntime();
            var actualVersion = SFEnvironment.ExtractVersion();

            // Assert
            Assert.AreEqual(expectedRuntime, actualRuntime);
            Assert.AreEqual(expectedVersion, actualVersion);
        }
    }
}
