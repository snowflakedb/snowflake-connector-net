using System;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session;

public sealed class SFSessionCRLPropertiesExtractorTest
{
    [SFFact]
    public void TestExtractsDefaultCRLProperties()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test");
        var extractor = new SFSessionCRLProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(CertRevocationCheckMode.Disabled, result.CertRevocationCheckMode);
        Assert.True(result.EnableCrlDiskCaching);
        Assert.True(result.EnableCrlInMemoryCaching);
        Assert.False(result.AllowCertificatesWithoutCrlUrl);
        Assert.Equal(10, result.CRLDownloadTimeout);
        Assert.Equal(20971520L, result.CRLDownloadMaxSize);
    }

    [SFTheory]
    [InlineData("enabled")]
    [InlineData("advisory")]
    [InlineData("disabled")]
    [InlineData("native")]
    public void TestExtractsCertRevocationCheckMode(string mode)
    {
        // arrange
        var props = ParseConnectionString($"ACCOUNT=account;USER=test;PASSWORD=test;CERTREVOCATIONCHECKMODE={mode}");
        var extractor = new SFSessionCRLProperties.Extractor();
        var expected = (CertRevocationCheckMode)Enum.Parse(typeof(CertRevocationCheckMode), mode, true);

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(expected, result.CertRevocationCheckMode);
    }

    [SFTheory]
    [InlineData("false", false)]
    [InlineData("true", true)]
    public void TestExtractsCrlDiskCaching(string value, bool expected)
    {
        // arrange
        var props = ParseConnectionString($"ACCOUNT=account;USER=test;PASSWORD=test;ENABLECRLDISKCACHING={value}");
        var extractor = new SFSessionCRLProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(expected, result.EnableCrlDiskCaching);
    }

    [SFTheory]
    [InlineData("false", false)]
    [InlineData("true", true)]
    public void TestExtractsCrlInMemoryCaching(string value, bool expected)
    {
        // arrange
        var props = ParseConnectionString($"ACCOUNT=account;USER=test;PASSWORD=test;ENABLECRLINMEMORYCACHING={value}");
        var extractor = new SFSessionCRLProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(expected, result.EnableCrlInMemoryCaching);
    }

    [SFTheory]
    [InlineData("false", false)]
    [InlineData("true", true)]
    public void TestExtractsAllowCertificatesWithoutCrlUrl(string value, bool expected)
    {
        // arrange
        var props = ParseConnectionString($"ACCOUNT=account;USER=test;PASSWORD=test;ALLOWCERTIFICATESWITHOUTCRLURL={value}");
        var extractor = new SFSessionCRLProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(expected, result.AllowCertificatesWithoutCrlUrl);
    }

    [SFTheory]
    [InlineData(5)]
    [InlineData(30)]
    public void TestExtractsCrlDownloadTimeout(int timeout)
    {
        // arrange
        var props = ParseConnectionString($"ACCOUNT=account;USER=test;PASSWORD=test;CRLDOWNLOADTIMEOUT={timeout}");
        var extractor = new SFSessionCRLProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(timeout, result.CRLDownloadTimeout);
    }

    [SFTheory]
    [InlineData(100)]
    [InlineData(20971520)]
    public void TestExtractsCrlDownloadMaxSize(long maxSize)
    {
        // arrange
        var props = ParseConnectionString($"ACCOUNT=account;USER=test;PASSWORD=test;CRLDOWNLOADMAXSIZE={maxSize}");
        var extractor = new SFSessionCRLProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(maxSize, result.CRLDownloadMaxSize);
    }

    [SFFact]
    public void TestExtractsAllCustomCRLProperties()
    {
        // arrange
        var props = ParseConnectionString(
            "ACCOUNT=account;USER=test;PASSWORD=test;" +
            "CERTREVOCATIONCHECKMODE=advisory;ENABLECRLDISKCACHING=false;ENABLECRLINMEMORYCACHING=false;" +
            "ALLOWCERTIFICATESWITHOUTCRLURL=true;CRLDOWNLOADTIMEOUT=30;CRLDOWNLOADMAXSIZE=5000000");
        var extractor = new SFSessionCRLProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(CertRevocationCheckMode.Advisory, result.CertRevocationCheckMode);
        Assert.False(result.EnableCrlDiskCaching);
        Assert.False(result.EnableCrlInMemoryCaching);
        Assert.True(result.AllowCertificatesWithoutCrlUrl);
        Assert.Equal(30, result.CRLDownloadTimeout);
        Assert.Equal(5000000L, result.CRLDownloadMaxSize);
    }

    private static SFSessionProperties ParseConnectionString(string connectionString)
        => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());
}
