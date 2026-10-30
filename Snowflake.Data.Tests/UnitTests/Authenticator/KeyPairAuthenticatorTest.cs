using System;
using Xunit;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator;

public sealed class KeyPairAuthenticatorTest
{
    [SFTheory]
    [InlineData("snowflake_jwt", true)]
    [InlineData("SNOWFLAKE_JWT", true)]
    [InlineData("username_password_mfa", false)]
    public void TestRecognizeKeyPairAuthenticator(string authenticator, bool expectedResult)
    {
        // act
        var result = KeyPairAuthenticator.IsKeyPairAuthenticator(authenticator);

        // assert
        Assert.Equal(expectedResult, result);
    }

    [SFFact]
    public void TestBuildPemDiagnosticIncludesNewlineHint()
    {
        var ex = new Exception("PemReader failed");
        var result = KeyPairAuthenticator.BuildPemDiagnostic(ex, "path", "content");

        Assert.Contains("real newline characters", result.Message);
        Assert.Contains("not the literal two-character sequence \\n", result.Message);
    }

    [SFFact]
    public void TestBuildPemDiagnosticDetectsPadBlockCorrupted()
    {
        var ex = new Exception("pad block corrupted");
        var result = KeyPairAuthenticator.BuildPemDiagnostic(ex, "path", "content");

        Assert.Contains("PRIVATE_KEY_PWD", result.Message);
        Assert.Contains("encrypted or the passphrase is incorrect", result.Message);
    }

    [SFFact]
    public void TestBuildPemDiagnosticDetectsInnerExceptionPadBlock()
    {
        var inner = new Exception("pad block corrupted");
        var outer = new Exception("Could not parse PEM", inner);
        var result = KeyPairAuthenticator.BuildPemDiagnostic(outer, "path", "content");

        Assert.Contains("PRIVATE_KEY_PWD", result.Message);
    }

    [SFTheory]
    [InlineData(null)]
    [InlineData("")]
    public void TestBuildPemDiagnosticReturnsInlineSourceWhenPathEmpty(string pkPath)
    {
        var ex = new Exception("some error");
        var result = KeyPairAuthenticator.BuildPemDiagnostic(ex, pkPath, "content");

        Assert.Equal("inline PRIVATE_KEY connection-string value", result.Source);
    }

    [SFTheory]
    [InlineData(null)]
    [InlineData("")]
    public void TestBuildPemDiagnosticReturnsRawExceptionWhenContentEmpty(string pkContent)
    {
        var ex = new Exception("some error");
        var result = KeyPairAuthenticator.BuildPemDiagnostic(ex, "path", pkContent);

        Assert.Equal(ex.ToString(), result.Message);
        Assert.Equal("path", result.Source);
    }

    [SFTheory]
    [InlineData("pkcs12 data found")]
    [InlineData("decryption failed at offset 42")]
    public void TestBuildPemDiagnosticDetectsOtherPasswordFragments(string exceptionMessage)
    {
        var ex = new Exception(exceptionMessage);
        var result = KeyPairAuthenticator.BuildPemDiagnostic(ex, "path", "content");

        Assert.Contains("PRIVATE_KEY_PWD", result.Message);
        Assert.Contains("encrypted or the passphrase is incorrect", result.Message);
    }
}
