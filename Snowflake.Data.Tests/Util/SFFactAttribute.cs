using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Xunit;

#if NET8_0_OR_GREATER
using Xunit.v3;
#else
using Xunit.Sdk;
#endif

namespace Snowflake.Data.Tests.Util;

#if NET8_0_OR_GREATER
[XunitTestCaseDiscoverer(typeof(SFTestCaseDiscovererV3))]
#else
[XunitTestCaseDiscoverer($"Snowflake.Data.Tests.Util.{nameof(SFTestCaseDiscoverer)}", "Snowflake.Data.Tests")]
#endif
public sealed class SFFactAttribute : FactAttribute
{
    public bool DedicatedSessionPool { get; }

    public RetriesCount RetriesCount { get; set; }

    public SFFactAttribute(SkipCondition skip = SkipCondition.None, bool dedicatedSessionPool = false, RetriesCount retriesCount = RetriesCount.Once, [CallerFilePath] string sourceFilePath = null, [CallerLineNumber] int sourceLineNumber = -1)
#if NET8_0_OR_GREATER
        : base(sourceFilePath, sourceLineNumber)
#endif
    {
        RetriesCount = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("JENKINS_HOME")) ? RetriesCount.Thrice : retriesCount;
        DedicatedSessionPool = dedicatedSessionPool;
        var skipEvaluationResult = SkipConditionEvaluator.Evaluate(skip);

        if (skipEvaluationResult.ShouldSkip)
            Skip = skipEvaluationResult.SkipMessage;
    }
}

#if NET8_0_OR_GREATER
[XunitTestCaseDiscoverer(typeof(SFTheoryDiscovererV3))]
#else
[XunitTestCaseDiscoverer($"Snowflake.Data.Tests.Util.{nameof(SFTheoryDiscoverer)}", "Snowflake.Data.Tests")]
#endif
public sealed class SFTheoryAttribute : TheoryAttribute
{
    public bool DedicatedSessionPool { get; }

    public RetriesCount RetriesCount { get; set; }

    public SFTheoryAttribute(SkipCondition skip = SkipCondition.None, bool dedicatedSessionPool = false, RetriesCount retriesCount = RetriesCount.Once, [CallerFilePath] string sourceFilePath = null, [CallerLineNumber] int sourceLineNumber = -1)
#if NET8_0_OR_GREATER
        : base(sourceFilePath, sourceLineNumber)
#endif
    {
        DedicatedSessionPool = dedicatedSessionPool;
        RetriesCount = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("JENKINS_HOME")) ? RetriesCount.Thrice : retriesCount;
        var skipEvaluationResult = SkipConditionEvaluator.Evaluate(skip);

        if (skipEvaluationResult.ShouldSkip)
            Skip = skipEvaluationResult.SkipMessage;
    }
}

public enum RetriesCount
{
    NoRetries = 0,
    Once = 1,
    Twice = 2,
    Thrice = 3
}

internal static class SkipConditionEvaluator
{
    internal readonly struct SkipEvaluationResult
    {
        internal readonly string SkipMessage;
        internal readonly bool ShouldSkip;

        public SkipEvaluationResult(string skipMessage, bool shouldSkip)
        {
            SkipMessage = skipMessage;
            ShouldSkip = shouldSkip;
        }
    }

    internal static SkipEvaluationResult Evaluate(SkipCondition condition)
    {
        var skipMessage = EvaluateInner(condition);
        return new(skipMessage, !string.IsNullOrEmpty(skipMessage));
    }

    private static string EvaluateInner(SkipCondition condition)
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

        // CI / Jenkins / Local checks
        if (condition.HasFlag(SkipCondition.SkipOnCI) && Environment.GetEnvironmentVariable("CI") == "true")
            return "Test is skipped on CI.";

        if (condition.HasFlag(SkipCondition.RunOnlyOnCI) && string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CI")))
            return "Test runs only on CI.";

        if (condition.HasFlag(SkipCondition.SkipOnJenkins) && !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("JENKINS_HOME")))
            return "Test is skipped on Jenkins.";

        if (condition.HasFlag(SkipCondition.SkipOnLocal) && Environment.GetEnvironmentVariable("CI") != "true" && string.IsNullOrEmpty(Environment.GetEnvironmentVariable("JENKINS_HOME")))
            return "Test is skipped on local environment.";

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
    SkipOnLinux = 1 << 1,
    SkipOnMacOS = 1 << 2,

    // Platform composites
    RunOnlyOnWindows = SkipOnLinux | SkipOnMacOS,
    RunOnlyOnLinux = SkipOnWindows | SkipOnMacOS,
    RunOnlyOnMacOS = SkipOnWindows | SkipOnLinux,

    // CI / Jenkins / Local
    SkipOnCI = 1 << 3,
    RunOnlyOnCI = 1 << 4,
    SkipOnJenkins = 1 << 5,
    SkipOnLocal = 1 << 6,
    RunOnlyOnLocal = SkipOnCI | SkipOnJenkins,

    // Cloud provider (reads snowflake_cloud_env)
    SkipOnCloudAWS = 1 << 7,
    SkipOnCloudAzure = 1 << 8,
    SkipOnCloudGCP = 1 << 9,
    RunOnlyOnCloudAWS = SkipOnCloudAzure | SkipOnCloudGCP,
    RunOnlyOnCloudAzure = SkipOnCloudAWS | SkipOnCloudGCP,
    RunOnlyOnCloudGCP = SkipOnCloudAWS | SkipOnCloudAzure,
}
