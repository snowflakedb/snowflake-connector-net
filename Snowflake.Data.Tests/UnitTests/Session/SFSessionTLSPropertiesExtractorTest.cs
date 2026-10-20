using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session;

public sealed class SFSessionTLSPropertiesExtractorTest
{
    [SFFact]
    public void TestExtractsDefaultTLSProperties()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test");
        var extractor = new SFSessionTLSProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal("tls12", result.MinTlsProtocol);
        Assert.Equal("tls13", result.MaxTlsProtocol);
        Assert.False(result.TlsProtocolsExplicitlyRequested);
        Assert.Null(result.TlsCipherPolicy);
    }

    [SFTheory]
    [InlineData("tls12", "tls12")]
    [InlineData("tls13", "tls13")]
    public void TestExtractsMinTlsProtocol(string minTls, string expectedMinTls)
    {
        // arrange
        var props = ParseConnectionString($"ACCOUNT=account;USER=test;PASSWORD=test;MINTLS={minTls}");
        var extractor = new SFSessionTLSProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(expectedMinTls, result.MinTlsProtocol);
    }

    [SFTheory]
    [InlineData("tls12", "tls12")]
    [InlineData("tls13", "tls13")]
    public void TestExtractsMaxTlsProtocol(string maxTls, string expectedMaxTls)
    {
        // arrange
        var props = ParseConnectionString($"ACCOUNT=account;USER=test;PASSWORD=test;MAXTLS={maxTls}");
        var extractor = new SFSessionTLSProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal(expectedMaxTls, result.MaxTlsProtocol);
    }

    [SFFact]
    public void TestExplicitlyRequestedWhenMinTlsProvided()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;MINTLS=tls13");
        var extractor = new SFSessionTLSProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.True(result.TlsProtocolsExplicitlyRequested);
    }

    [SFFact]
    public void TestExplicitlyRequestedWhenMaxTlsProvided()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;MAXTLS=tls12");
        var extractor = new SFSessionTLSProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.True(result.TlsProtocolsExplicitlyRequested);
    }

    [SFFact]
    public void TestExplicitlyRequestedWhenBothTlsProvided()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;MINTLS=tls12;MAXTLS=tls13");
        var extractor = new SFSessionTLSProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.True(result.TlsProtocolsExplicitlyRequested);
        Assert.Equal("tls12", result.MinTlsProtocol);
        Assert.Equal("tls13", result.MaxTlsProtocol);
    }

    [SFFact]
    public void TestNotExplicitlyRequestedWhenNoTlsProvided()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test");
        var extractor = new SFSessionTLSProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.False(result.TlsProtocolsExplicitlyRequested);
    }

    private static SFSessionProperties ParseConnectionString(string connectionString)
        => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());
}
