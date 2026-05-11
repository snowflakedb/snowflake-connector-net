using System;
using System.Runtime.InteropServices;
using Xunit;

namespace Snowflake.Data.Tests.Util;

public sealed class FactRunOnPlatformAttribute : FactAttribute
{
    public static OSPlatform GetOsPlatform(KnownOSPlatform osPlatform)
    {
        return osPlatform switch
        {
            KnownOSPlatform.Windows => OSPlatform.Windows,
            KnownOSPlatform.Linux => OSPlatform.Linux,
            KnownOSPlatform.MacOS => OSPlatform.OSX,
            _ => throw new ArgumentException("")
        };
    }

    public enum KnownOSPlatform
    {
        Windows,
        Linux,
        MacOS
    }

    public FactRunOnPlatformAttribute(KnownOSPlatform runOnPlatform)
    {
        var osPlatform = GetOsPlatform(runOnPlatform);

        if (RuntimeInformation.IsOSPlatform(osPlatform))
            return;

        Skip = $"This test is run only on {runOnPlatform}.";
    }
}

