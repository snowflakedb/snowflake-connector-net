using System.Runtime.InteropServices;
using Xunit;

namespace Snowflake.Data.Tests.Util;

public sealed class TheoryRunOnPlatformAttribute : TheoryAttribute
{
    public TheoryRunOnPlatformAttribute(FactRunOnPlatformAttribute.KnownOSPlatform runOnPlatform)
    {
        var osPlatform = FactRunOnPlatformAttribute.GetOsPlatform(runOnPlatform);

        if (RuntimeInformation.IsOSPlatform(osPlatform))
            return;

        Skip = $"This test is run only on {runOnPlatform}.";
    }
}
