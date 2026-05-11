using Xunit;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public sealed class LibcDetectorTest
    {
        [Fact]
        public void TestGetLibcFamilyStringForGlibc()
        {
            var result = LibcFamily.Glibc.ToPrettyString();
            Assert.Equal("glibc", result);
        }

        [Fact]
        public void TestGetLibcFamilyStringForNotApplicable()
        {
            var result = LibcFamily.NotApplicable.ToPrettyString();
            Assert.Null(result);
        }

        [Fact]
        public void TestGetLibcFamilyStringForCouldNotDetermine()
        {
            var result = LibcFamily.CouldNotDetermine.ToPrettyString();
            Assert.Equal("could not determine", result);
        }
    }
}
