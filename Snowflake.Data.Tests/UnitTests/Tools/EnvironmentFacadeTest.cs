using System;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Tools;

[CollectionDefinition(nameof(EnvironmentFacadeTestFixture), DisableParallelization = true)]
public sealed class EnvironmentFacadeTestFixture { }

[Collection(nameof(EnvironmentFacadeTestFixture))]
public sealed class EnvironmentFacadeTest : IDisposable
{
    private const string TestEnvVar = "SF_TEST_ENV_FACADE_UNIT_TEST";

    private static readonly EnvVar<TimeSpan> s_timeSpanVar = new(TestEnvVar, TimeSpan.FromSeconds(42), EnvVarParseMode.FromSeconds);
    private static readonly EnvVar<int> s_intVar = new(TestEnvVar, 99);
    private static readonly EnvVar<bool> s_boolVar = new(TestEnvVar, false);
    private static readonly EnvVar<string> s_stringVar = new(TestEnvVar, "default_val");

    private readonly EnvironmentFacade _sut = new();

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, null);
    }

    // --- GetTimeSpan ---

    [SFFact]
    public void TestGetTimeSpanReturnsDefaultWhenEnvVarNotSet()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, null);

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestGetTimeSpanReturnsDefaultWhenEnvVarIsEmpty()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "");

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestGetTimeSpanParsesValidSeconds()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "300");

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(300), result);
    }

    [SFFact]
    public void TestGetTimeSpanParsesZero()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "0");

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.Zero, result);
    }

    [SFFact]
    public void TestGetTimeSpanReturnsDefaultWhenValueIsNonNumeric()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "abc");

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestGetTimeSpanReturnsDefaultWhenValueIsFloat()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "3.5");

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestGetTimeSpanParsesNegativeValue()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "-1");

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(-1), result);
    }

    [SFFact]
    public void TestGetTimeSpanReturnsDefaultOnIntOverflow()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "99999999999999");

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFFact]
    public void TestGetTimeSpanReturnsDefaultWhenParseModeIsDefault()
    {
        var varWithDefaultMode = new EnvVar<TimeSpan>(TestEnvVar, TimeSpan.FromMinutes(5), EnvVarParseMode.Default);
        Environment.SetEnvironmentVariable(TestEnvVar, "300");

        var result = _sut.GetTimeSpan(varWithDefaultMode);

        Assert.Equal(TimeSpan.FromMinutes(5), result);
    }

    [SFFact]
    public void TestGetTimeSpanReturnsDefaultWhenValueIsWhitespace()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "  ");

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(42), result);
    }

    [SFTheory]
    [InlineData("1", 1)]
    [InlineData("60", 60)]
    [InlineData("180", 180)]
    [InlineData("3600", 3600)]
    public void TestGetTimeSpanParsesVariousValidValues(string envValue, int expectedSeconds)
    {
        Environment.SetEnvironmentVariable(TestEnvVar, envValue);

        var result = _sut.GetTimeSpan(s_timeSpanVar);

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), result);
    }

    // --- GetInt ---

    [SFFact]
    public void TestGetIntReturnsDefaultWhenEnvVarNotSet()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, null);

        var result = _sut.GetInt(s_intVar);

        Assert.Equal(99, result);
    }

    [SFFact]
    public void TestGetIntReturnsDefaultWhenEnvVarIsEmpty()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "");

        var result = _sut.GetInt(s_intVar);

        Assert.Equal(99, result);
    }

    [SFFact]
    public void TestGetIntParsesValidValue()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "7");

        var result = _sut.GetInt(s_intVar);

        Assert.Equal(7, result);
    }

    [SFFact]
    public void TestGetIntParsesZero()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "0");

        var result = _sut.GetInt(s_intVar);

        Assert.Equal(0, result);
    }

    [SFFact]
    public void TestGetIntParsesNegativeValue()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "-5");

        var result = _sut.GetInt(s_intVar);

        Assert.Equal(-5, result);
    }

    [SFFact]
    public void TestGetIntReturnsDefaultWhenValueIsNonNumeric()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "xyz");

        var result = _sut.GetInt(s_intVar);

        Assert.Equal(99, result);
    }

    [SFFact]
    public void TestGetIntReturnsDefaultOnOverflow()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "99999999999999");

        var result = _sut.GetInt(s_intVar);

        Assert.Equal(99, result);
    }

    // --- GetBool ---

    [SFFact]
    public void TestGetBoolReturnsDefaultWhenEnvVarNotSet()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, null);

        var result = _sut.GetBool(s_boolVar);

        Assert.False(result);
    }

    [SFFact]
    public void TestGetBoolParsesTrue()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "true");

        var result = _sut.GetBool(s_boolVar);

        Assert.True(result);
    }

    [SFFact]
    public void TestGetBoolParsesFalse()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "false");

        var result = _sut.GetBool(s_boolVar);

        Assert.False(result);
    }

    [SFFact]
    public void TestGetBoolParsesTrueCaseInsensitive()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "True");

        var result = _sut.GetBool(s_boolVar);

        Assert.True(result);
    }

    [SFFact]
    public void TestGetBoolReturnsDefaultWhenValueIsInvalid()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "yes");

        var result = _sut.GetBool(s_boolVar);

        Assert.False(result);
    }

    [SFFact]
    public void TestGetBoolReturnsDefaultWhenValueIsNumeric()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "1");

        var result = _sut.GetBool(s_boolVar);

        Assert.False(result);
    }

    // --- GetString ---

    [SFFact]
    public void TestGetStringReturnsDefaultWhenEnvVarNotSet()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, null);

        var result = _sut.GetString(s_stringVar);

        Assert.Equal("default_val", result);
    }

    [SFFact]
    public void TestGetStringReturnsDefaultWhenEnvVarIsEmpty()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "");

        var result = _sut.GetString(s_stringVar);

        Assert.Equal("default_val", result);
    }

    [SFFact]
    public void TestGetStringReturnsValue()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "hello");

        var result = _sut.GetString(s_stringVar);

        Assert.Equal("hello", result);
    }

    [SFFact]
    public void TestGetStringReturnsValueWithWhitespace()
    {
        Environment.SetEnvironmentVariable(TestEnvVar, "  spaces  ");

        var result = _sut.GetString(s_stringVar);

        Assert.Equal("  spaces  ", result);
    }
}
