using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session;

public sealed class SFSessionHttpClientProxyPropertiesExtractorTest
{
    [SFFact]
    public void TestExtractsNullPropertiesWhenProxyDisabled()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;USEPROXY=false");
        var extractor = new SFSessionHttpClientProxyProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Null(result.proxyHost);
        Assert.Null(result.proxyPort);
        Assert.Null(result.nonProxyHosts);
        Assert.Null(result.proxyPassword);
        Assert.Null(result.proxyUser);
    }

    [SFFact]
    public void TestExtractsNullPropertiesWhenProxyNotSpecified()
    {
        // arrange - default for USEPROXY is false
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test");
        var extractor = new SFSessionHttpClientProxyProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Null(result.proxyHost);
        Assert.Null(result.proxyPort);
        Assert.Null(result.nonProxyHosts);
        Assert.Null(result.proxyPassword);
        Assert.Null(result.proxyUser);
    }

    [SFFact]
    public void TestExtractsProxyPropertiesWhenProxyEnabled()
    {
        // arrange
        var props = ParseConnectionString(
            "ACCOUNT=account;USER=test;PASSWORD=test;USEPROXY=true;" +
            "PROXYHOST=myhost;PROXYPORT=8080;PROXYUSER=user;PROXYPASSWORD=pass");
        var extractor = new SFSessionHttpClientProxyProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal("myhost", result.proxyHost);
        Assert.Equal("8080", result.proxyPort);
        Assert.Equal("user", result.proxyUser);
        Assert.Equal("pass", result.proxyPassword);
    }

    [SFFact]
    public void TestDecodesNonProxyHosts()
    {
        // arrange - pipe character is URL-encoded as %7C in connection strings
        var props = ParseConnectionString(
            "ACCOUNT=account;USER=test;PASSWORD=test;USEPROXY=true;" +
            "PROXYHOST=myhost;PROXYPORT=8080;NONPROXYHOSTS=localhost%7Csnowflake.com");
        var extractor = new SFSessionHttpClientProxyProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal("localhost|snowflake.com", result.nonProxyHosts);
    }

    [SFFact]
    public void TestNonProxyHostsNullWhenNotSet()
    {
        // arrange
        var props = ParseConnectionString(
            "ACCOUNT=account;USER=test;PASSWORD=test;USEPROXY=true;" +
            "PROXYHOST=myhost;PROXYPORT=8080");
        var extractor = new SFSessionHttpClientProxyProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Null(result.nonProxyHosts);
    }

    [SFFact]
    public void TestExtractsProxyWithPartialConfig()
    {
        // arrange - only host specified, other fields empty
        var props = ParseConnectionString(
            "ACCOUNT=account;USER=test;PASSWORD=test;USEPROXY=true;PROXYHOST=myhost");
        var extractor = new SFSessionHttpClientProxyProperties.Extractor();

        // act
        var result = extractor.ExtractProperties(props);

        // assert
        Assert.Equal("myhost", result.proxyHost);
    }

    private static SFSessionProperties ParseConnectionString(string connectionString)
        => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());
}
