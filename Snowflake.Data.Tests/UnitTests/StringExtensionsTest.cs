using System;
using Xunit;
using Snowflake.Data.Core.Extensions;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests;

[CollectionDefinition(nameof(StringExtensionsTestCollection), DisableParallelization = true)]
public sealed class StringExtensionsTestCollection;

[Collection(nameof(StringExtensionsTestCollection))]
public sealed class StringExtensionsTest : IDisposable
{
    public void Dispose() => SecretDetector.ClearCustomPatterns();

    [SFTheory]
    [InlineData(null, null)]
    [InlineData("", "")]
    [InlineData("hello world", "hello world")]
    [InlineData("password=MySecret123", "password=****")]
    [InlineData("token='abcdefgh12345678'", "token=****")]
    public void TestStringToMaskedString(string input, string expected)
    {
        var result = input.ToMaskedString();

        Assert.Equal(expected, result);
    }

    [SFTheory]
    [InlineData("https://example.com/path?key=value", "https://example.com/path?key=value")]
    [InlineData("https://example.com?password=MySecret123", "https://example.com/?password=****")]
    public void TestUriToMaskedString(string url, string expected)
    {
        var result = new Uri(url).ToMaskedString();

        Assert.Equal(expected, result);
    }

    [SFFact]
    public void TestNullUriToMaskedStringReturnsNull()
    {
        Uri uri = null;

        Assert.Null(uri.ToMaskedString());
    }

    [SFFact]
    public void TestToMaskedStringReturnsErrorWhenMaskingFails()
    {
        SecretDetector.SetCustomPatterns(
            ["[invalid regex"],
            ["****"]);

        var result = "some text to mask".ToMaskedString();

        Assert.StartsWith("[Error occurred during secret masking]:", result);
    }
}
