using System;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

[CollectionDefinition(nameof(SkipConditionEvaluatorTest), DisableParallelization = true)]
public sealed class SkipConditionEvaluatorTestFixture : ICollectionFixture<SkipConditionEvaluatorTestFixture> { }

[Collection(nameof(SkipConditionEvaluatorTest))]
public sealed class SkipConditionEvaluatorTest : IDisposable
{
    private readonly string _originalCI;
    private readonly string _originalJenkinsHome;
    private readonly string _originalCloudEnv;

    public SkipConditionEvaluatorTest()
    {
        _originalCI = Environment.GetEnvironmentVariable("CI");
        _originalJenkinsHome = Environment.GetEnvironmentVariable("JENKINS_HOME");
        _originalCloudEnv = Environment.GetEnvironmentVariable("snowflake_cloud_env");
        ClearEnvironmentVariables();
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable("CI", _originalCI);
        Environment.SetEnvironmentVariable("JENKINS_HOME", _originalJenkinsHome);
        Environment.SetEnvironmentVariable("snowflake_cloud_env", _originalCloudEnv);
    }

    private static void ClearEnvironmentVariables()
    {
        Environment.SetEnvironmentVariable("CI", null);
        Environment.SetEnvironmentVariable("JENKINS_HOME", null);
        Environment.SetEnvironmentVariable("snowflake_cloud_env", null);
    }

    [SFFact]
    public void TestNoneConditionDoesNotSkip()
    {
        var result = SkipConditionEvaluator.Evaluate(SkipCondition.None);

        Assert.False(result.ShouldSkip);
    }

    // CI checks

    [SFFact]
    public void TestSkipOnCI_WhenOnCI_Skips()
    {
        Environment.SetEnvironmentVariable("CI", "true");

        var result = SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnCI);

        Assert.True(result.ShouldSkip);
        Assert.Contains("CI", result.SkipMessage);
    }

    [SFFact]
    public void TestSkipOnCI_WhenNotOnCI_DoesNotSkip()
    {
        var result = SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnCI);

        Assert.False(result.ShouldSkip);
    }

    [SFFact]
    public void TestRunOnlyOnCI_WhenOnCI_DoesNotSkip()
    {
        Environment.SetEnvironmentVariable("CI", "true");

        var result = SkipConditionEvaluator.Evaluate(SkipCondition.RunOnlyOnCI);

        Assert.False(result.ShouldSkip);
    }

    [SFFact]
    public void TestRunOnlyOnCI_WhenNotOnCI_Skips()
    {
        var result = SkipConditionEvaluator.Evaluate(SkipCondition.RunOnlyOnCI);

        Assert.True(result.ShouldSkip);
        Assert.Contains("only on CI", result.SkipMessage);
    }

    // Jenkins checks

    [SFFact]
    public void TestSkipOnJenkins_WhenOnJenkins_Skips()
    {
        Environment.SetEnvironmentVariable("JENKINS_HOME", "/var/jenkins");

        var result = SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnJenkins);

        Assert.True(result.ShouldSkip);
        Assert.Contains("Jenkins", result.SkipMessage);
    }

    [SFFact]
    public void TestSkipOnJenkins_WhenNotOnJenkins_DoesNotSkip()
    {
        var result = SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnJenkins);

        Assert.False(result.ShouldSkip);
    }

    // Local checks

    [SFFact]
    public void TestSkipOnLocal_WhenLocal_Skips()
    {
        var result = SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnLocal);

        Assert.True(result.ShouldSkip);
        Assert.Contains("local", result.SkipMessage);
    }

    [SFFact]
    public void TestSkipOnLocal_WhenOnCI_DoesNotSkip()
    {
        Environment.SetEnvironmentVariable("CI", "true");

        var result = SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnLocal);

        Assert.False(result.ShouldSkip);
    }

    [SFFact]
    public void TestSkipOnLocal_WhenOnJenkins_DoesNotSkip()
    {
        Environment.SetEnvironmentVariable("JENKINS_HOME", "/var/jenkins");

        var result = SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnLocal);

        Assert.False(result.ShouldSkip);
    }

    [SFFact]
    public void TestRunOnlyOnLocal_WhenLocal_DoesNotSkip()
    {
        var result = SkipConditionEvaluator.Evaluate(SkipCondition.RunOnlyOnLocal);

        Assert.False(result.ShouldSkip);
    }

    [SFFact]
    public void TestRunOnlyOnLocal_WhenOnCI_Skips()
    {
        Environment.SetEnvironmentVariable("CI", "true");

        var result = SkipConditionEvaluator.Evaluate(SkipCondition.RunOnlyOnLocal);

        Assert.True(result.ShouldSkip);
    }

    [SFFact]
    public void TestRunOnlyOnLocal_WhenOnJenkins_Skips()
    {
        Environment.SetEnvironmentVariable("JENKINS_HOME", "/var/jenkins");

        var result = SkipConditionEvaluator.Evaluate(SkipCondition.RunOnlyOnLocal);

        Assert.True(result.ShouldSkip);
    }

    // Cloud provider checks

    [SFTheory]
    [InlineData("AWS", SkipCondition.SkipOnCloudAWS)]
    [InlineData("AZURE", SkipCondition.SkipOnCloudAzure)]
    [InlineData("GCP", SkipCondition.SkipOnCloudGCP)]
    public void TestSkipOnCloud_WhenMatchingCloud_Skips(string cloud, SkipCondition condition)
    {
        Environment.SetEnvironmentVariable("snowflake_cloud_env", cloud);

        var result = SkipConditionEvaluator.Evaluate(condition);

        Assert.True(result.ShouldSkip);
    }

    [SFTheory]
    [InlineData("AZURE", SkipCondition.SkipOnCloudAWS)]
    [InlineData("GCP", SkipCondition.SkipOnCloudAWS)]
    [InlineData("AWS", SkipCondition.SkipOnCloudAzure)]
    [InlineData("GCP", SkipCondition.SkipOnCloudAzure)]
    [InlineData("AWS", SkipCondition.SkipOnCloudGCP)]
    [InlineData("AZURE", SkipCondition.SkipOnCloudGCP)]
    public void TestSkipOnCloud_WhenDifferentCloud_DoesNotSkip(string cloud, SkipCondition condition)
    {
        Environment.SetEnvironmentVariable("snowflake_cloud_env", cloud);

        var result = SkipConditionEvaluator.Evaluate(condition);

        Assert.False(result.ShouldSkip);
    }

    [SFTheory]
    [InlineData("AWS", SkipCondition.RunOnlyOnCloudAWS)]
    [InlineData("AZURE", SkipCondition.RunOnlyOnCloudAzure)]
    [InlineData("GCP", SkipCondition.RunOnlyOnCloudGCP)]
    public void TestRunOnlyOnCloud_WhenMatchingCloud_DoesNotSkip(string cloud, SkipCondition condition)
    {
        Environment.SetEnvironmentVariable("snowflake_cloud_env", cloud);

        var result = SkipConditionEvaluator.Evaluate(condition);

        Assert.False(result.ShouldSkip);
    }

    [SFTheory]
    [InlineData("AZURE", SkipCondition.RunOnlyOnCloudAWS)]
    [InlineData("AWS", SkipCondition.RunOnlyOnCloudAzure)]
    [InlineData("AWS", SkipCondition.RunOnlyOnCloudGCP)]
    public void TestRunOnlyOnCloud_WhenDifferentCloud_Skips(string cloud, SkipCondition condition)
    {
        Environment.SetEnvironmentVariable("snowflake_cloud_env", cloud);

        var result = SkipConditionEvaluator.Evaluate(condition);

        Assert.True(result.ShouldSkip);
    }
}
