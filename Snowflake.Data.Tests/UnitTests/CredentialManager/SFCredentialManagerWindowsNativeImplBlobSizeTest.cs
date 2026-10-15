using System;
using System.Text;
using Snowflake.Data.Client;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager;

public sealed class SFCredentialManagerWindowsNativeImplBlobSizeTest : IDisposable
{
    private const int TokenLimitBytes = 5 * 512;
    private const string Key1 = "control-small-token";
    private const string Key2 = "oversized-token";
    private readonly ISnowflakeCredentialManager _credentialManager;

    public SFCredentialManagerWindowsNativeImplBlobSizeTest()
    {
        _credentialManager = new SFCredentialManagerWindowsNativeImpl();
    }

    [SFTheory(SkipCondition.RunOnlyOnWindows)]
    [InlineData(TokenLimitBytes / 2)]
    [InlineData((TokenLimitBytes - 1) / 2)]
    [InlineData(16)]
    public void TestControlCharTokenRoundTrips(int tokenLength)
    {
        var token = new string('a', tokenLength);
        var blobBytes = Encoding.Unicode.GetBytes(token).Length;

        Assert.True(blobBytes <= TokenLimitBytes,
            "test setup error: control token must be under the cap");

        _credentialManager.SaveCredentials(Key1, token);
        Assert.Equal(token, _credentialManager.GetCredentials(Key1));
    }

    [SFTheory(SkipCondition.RunOnlyOnWindows)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10000)]
    public void TestOversizedTokenIsSkippedWithoutThrowing(int charsExceedingTheLimit)
    {
        var oversizedToken = new string('A', TokenLimitBytes / 2 + charsExceedingTheLimit);
        var blobBytes = Encoding.Unicode.GetBytes(oversizedToken).Length;

        Assert.True(blobBytes > TokenLimitBytes,
            "test setup error: blob must exceed the cap");

        _credentialManager.SaveCredentials(Key2, oversizedToken);
        Assert.Equal(string.Empty, _credentialManager.GetCredentials(Key2));
    }

    public void Dispose()
    {
        foreach (var key in (string[])[Key1, Key2])
        {
            _credentialManager.RemoveCredentials(key);
        }
    }
}
