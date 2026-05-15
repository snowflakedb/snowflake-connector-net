using System;
using System.Runtime.InteropServices;
using Xunit;

#if NET8_0_OR_GREATER
using Xunit.v3;
#else
using Xunit.Sdk;
#endif

namespace Snowflake.Data.Tests.Util;

#if NET8_0_OR_GREATER
[XunitTestCaseDiscoverer(typeof(XunitTestCaseDiscoverer))]
#else
[XunitTestCaseDiscoverer($"Snowflake.Data.Tests.Util.{nameof(XunitTestCaseDiscoverer)}", "Snowflake.Data.Tests")]
#endif
public sealed class SFFactAttribute : FactAttribute
{
    public SFFactAttribute(SkipCondition skip = SkipCondition.None)
    {
        Skip = SkipConditionEvaluator.Evaluate(skip);
        Timeout = (int)TimeSpan.FromMinutes(15).TotalMilliseconds;
    }
}


public sealed class SFTheoryAttribute : TheoryAttribute
{
    public SFTheoryAttribute(SkipCondition skip = SkipCondition.None)
    {
        Skip = SkipConditionEvaluator.Evaluate(skip);
        Timeout = (int)TimeSpan.FromMinutes(15).TotalMilliseconds;
    }
}


internal static class SkipConditionEvaluator
{
    internal static string Evaluate(SkipCondition condition)
    {
        if (condition == SkipCondition.None)
            return null;

        // Platform checks
        if (condition.HasFlag(SkipCondition.SkipOnWindows) && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return "Test is skipped on Windows.";

        if (condition.HasFlag(SkipCondition.SkipOnLinux) && RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            return "Test is skipped on Linux.";

        if (condition.HasFlag(SkipCondition.SkipOnMacOS) && RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            return "Test is skipped on macOS.";

        // CI / Jenkins checks
        if (condition.HasFlag(SkipCondition.SkipOnCI) && Environment.GetEnvironmentVariable("CI") == "true")
            return "Test is skipped on CI.";

        if (condition.HasFlag(SkipCondition.RunOnlyOnCI) && string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CI")))
            return "Test runs only on CI.";

        if (condition.HasFlag(SkipCondition.SkipOnJenkins) && !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("JENKINS_HOME")))
            return "Test is skipped on Jenkins.";

        // Cloud provider checks
        var cloudEnv = Environment.GetEnvironmentVariable("snowflake_cloud_env");

        if (condition.HasFlag(SkipCondition.SkipOnCloudAWS) && cloudEnv == "AWS")
            return "Test is skipped on AWS.";

        if (condition.HasFlag(SkipCondition.SkipOnCloudAzure) && cloudEnv == "AZURE")
            return "Test is skipped on Azure.";

        if (condition.HasFlag(SkipCondition.SkipOnCloudGCP) && cloudEnv == "GCP")
            return "Test is skipped on GCP.";

        return null;
    }
}

[Flags]
public enum SkipCondition
{
    None = 0,

    // Platform (primitive bits)
    SkipOnWindows = 1 << 0,
    SkipOnLinux   = 1 << 1,
    SkipOnMacOS   = 1 << 2,

    // Platform composites
    RunOnlyOnWindows = SkipOnLinux   | SkipOnMacOS,
    RunOnlyOnLinux   = SkipOnWindows | SkipOnMacOS,
    RunOnlyOnMacOS   = SkipOnWindows | SkipOnLinux,

    // CI / Jenkins
    SkipOnCI      = 1 << 3,
    RunOnlyOnCI   = 1 << 4,
    SkipOnJenkins = 1 << 5,

    // Cloud provider (reads snowflake_cloud_env)
    SkipOnCloudAWS   = 1 << 6,
    SkipOnCloudAzure = 1 << 7,
    SkipOnCloudGCP   = 1 << 8,
    RunOnlyOnCloudAWS   = SkipOnCloudAzure | SkipOnCloudGCP,
    RunOnlyOnCloudAzure = SkipOnCloudAWS   | SkipOnCloudGCP,
    RunOnlyOnCloudGCP   = SkipOnCloudAWS   | SkipOnCloudAzure,
}
