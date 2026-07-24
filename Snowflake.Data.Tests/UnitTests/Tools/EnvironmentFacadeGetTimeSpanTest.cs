using System;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Tools;

public sealed class EnvironmentFacadeGetTimeSpanTest : IDisposable
{
    private const string TestEnvVar = "SF_TEST_TIMEOUT_FOR_UNIT_TEST";
    private static readonly EnvVar<TimeSpan> s_testVar = new(TestEnvVar, TimeSpan.FromSeconds(42), EnvVarParseMode.FromSeconds);

    private readonly EnvironmentFacade _sut = new();

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, null);
    }

    [SFFact]
    public void TestReturnsDefaultWhenEnvVarNotSet()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, null);

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestReturnsDefaultWhenEnvVarIsEmpty()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "");

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestParsesValidSeconds()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "300");

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(300), result);
    }

    [SFFact]
    public void TestParsesZeroSeconds()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "0");

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.Zero, result);
    }

    [SFFact]
    public void TestReturnsDefaultWhenValueIsNonNumeric()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "abc");

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestReturnsDefaultWhenValueIsFloat()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "3.5");

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestParsesNegativeValueAsSeconds()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "-1");

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(-1), result);
    }

    [SFFact]
    public void TestReturnsDefaultOnIntOverflow()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "99999999999999");

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestReturnsDefaultWhenParseModeIsDefault()
    {
        var varWithDefaultMode = new EnvVar<TimeSpan>(TestEnvVar, TimeSpan.FromMinutes(5), EnvVarParseMode.Default);
        Environment.SetEnvironmentVariable(TestEnvVar, "300");

        var result = _sut.GetTimeSpan(varWithDefaultMode);

        Assert.Equal(TimeSpan.FromMinutes(5), result);
    }

    [SFFact]
    public void TestReturnsDefaultWhenValueHasWhitespace()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "  ");

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFTheory]
    [InlineData("1", 1)]
    [InlineData("60", 60)]
    [InlineData("180", 180)]
    [InlineData("3600", 3600)]
    public void TestParsesVariousValidValues(string envValue, int expectedSeconds)
    {
        Environment.SetEnvironmentVariable(TestEnvVar, envValue);

        var result = _sut.GetTimeSpan(s_testVar);

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), result);
    }
}
