using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class TargetFrameworkReporterTest
    {
        [Test]
        public void TestTargetFramework()
        {
            TargetFrameworkReporter.Report();
        }
    }
}
