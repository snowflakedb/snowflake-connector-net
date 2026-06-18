using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public sealed class LibcDetectorTest
    {
        [SFFact]
        public void TestGetLibcFamilyStringForGlibc()
        {
            var result = LibcFamily.Glibc.ToPrettyString();
            Assert.Equal("glibc", result);
        }

        [SFFact]
        public void TestGetLibcFamilyStringForNotApplicable()
        {
            var result = LibcFamily.NotApplicable.ToPrettyString();
            Assert.Null(result);
        }

        [SFFact]
        public void TestGetLibcFamilyStringForCouldNotDetermine()
        {
            var result = LibcFamily.CouldNotDetermine.ToPrettyString();
            Assert.Equal("could not determine", result);
        }
    }
}
