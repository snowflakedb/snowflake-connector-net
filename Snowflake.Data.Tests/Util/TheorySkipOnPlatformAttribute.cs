using System.Runtime.InteropServices;
using Xunit;

namespace Snowflake.Data.Tests.Util;

public sealed class TheorySkipOnPlatformAttribute : TheoryAttribute
{
    public TheorySkipOnPlatformAttribute(FactRunOnPlatformAttribute.KnownOSPlatform skipOnPlatform)
    {
        var platform = FactRunOnPlatformAttribute.GetOsPlatform(skipOnPlatform);
        if (!RuntimeInformation.IsOSPlatform(platform))
            return;

        Skip = $"This test is skipped on {skipOnPlatform}.";
    }
}
