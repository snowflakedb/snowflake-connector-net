using Xunit;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{

    public sealed class LibcDetectorTest
    {
        [SFFact]
        public void TestGetLibcFamilyStringForGlibc()
        {
            var result = LibcFamily.Glibc.ToPrettyString();
            Assert.That(result, Is.EqualTo("glibc"));
        }

        [SFFact]
        public void TestGetLibcFamilyStringForNotApplicable()
        {
            var result = LibcFamily.NotApplicable.ToPrettyString();
            Assert.That(result, Is.Null);
        }

        [SFFact]
        public void TestGetLibcFamilyStringForCouldNotDetermine()
        {
            var result = LibcFamily.CouldNotDetermine.ToPrettyString();
            Assert.That(result, Is.EqualTo("could not determine"));
        }
    }
}
