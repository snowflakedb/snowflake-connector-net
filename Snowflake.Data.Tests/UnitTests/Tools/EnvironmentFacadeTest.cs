using System;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Tools;

[CollectionDefinition(nameof(EnvironmentFacadeTestFixture), DisableParallelization = true)]
public sealed class EnvironmentFacadeTestFixture;

[Collection(nameof(EnvironmentFacadeTestFixture))]
public sealed class EnvironmentFacadeTest : IDisposable
{
    private const string TestEnvVar = "SF_TEST_ENV_FACADE_UNIT_TEST";

    private static readonly EnvVar<TimeSpan> s_timeSpanVar = new(TestEnvVar, TimeSpan.FromSeconds(42), EnvVarParseMode.FromSeconds);
    private static readonly EnvVar<int> s_intVar = new(TestEnvVar, 99);
    private static readonly EnvVar<bool> s_boolVar = new(TestEnvVar, false);
    private static readonly EnvVar<string> s_stringVar = new(TestEnvVar, "default_val");

    private readonly EnvironmentFacade _facade = new();

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, null);
    }

    [SFTheory]
    [InlineData("1", 1)]
    [InlineData("0", 0)]
    [InlineData("-1", -1)]
    [InlineData("60", 60)]
    [InlineData("180", 180)]
    [InlineData("3600", 3600)]
    [InlineData(null, 42)]
    [InlineData("", 42)]
    [InlineData("  ", 42)]
    [InlineData("abc", 42)]
    [InlineData("3.5", 42)]
    [InlineData("99999999999999", 42)]
    public void TestGetTimeSpan(string envValue, int expectedSeconds)
    {
        Environment.SetEnvironmentVariable(TestEnvVar, envValue);

        var result = _facade.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), result);
    }

    [SFFact]
    public void TestGetTimeSpanWhenParseModeIsDefault()
    {
        var varWithDefaultMode = new EnvVar<TimeSpan>(TestEnvVar, TimeSpan.FromMinutes(5));
        Environment.SetEnvironmentVariable(TestEnvVar, "02:01:03");

        var result = _facade.GetTimeSpan(varWithDefaultMode);

        var expected = new TimeSpan(0, 2, 1, 3);
        Assert.Equal(expected, result);
    }

    [SFTheory]
    [InlineData("7", 7)]
    [InlineData("0", 0)]
    [InlineData("-5", -5)]
    [InlineData("2147483647", int.MaxValue)]
    [InlineData(null, 99)]
    [InlineData("", 99)]
    [InlineData("xyz", 99)]
    [InlineData("3.5", 99)]
    [InlineData("99999999999999", 99)]
    public void TestGetInt(string envValue, int expected)
    {
        Environment.SetEnvironmentVariable(TestEnvVar, envValue);

        var result = _facade.GetInt(s_intVar);

        Assert.Equal(expected, result);
    }

    [SFTheory]
    [InlineData("true", true)]
    [InlineData("True", true)]
    [InlineData("TRUE", true)]
    [InlineData("false", false)]
    [InlineData("False", false)]
    [InlineData(null, false)]
    [InlineData("", false)]
    [InlineData("yes", false)]
    [InlineData("1", false)]
    [InlineData("0", false)]
    [InlineData("on", false)]
    public void TestGetBool(string envValue, bool expected)
    {
        Environment.SetEnvironmentVariable(TestEnvVar, envValue);

        var result = _facade.GetBool(s_boolVar);

        Assert.Equal(expected, result);
    }

    [SFTheory]
    [InlineData("hello", "hello")]
    [InlineData("  spaces  ", "  spaces  ")]
    [InlineData("/some/path", "/some/path")]
    [InlineData(null, "default_val")]
    [InlineData("", "default_val")]
    public void TestGetString(string envValue, string expected)
    {
        Environment.SetEnvironmentVariable(TestEnvVar, envValue);

        var result = _facade.GetString(s_stringVar);

        Assert.Equal(expected, result);
    }
}
