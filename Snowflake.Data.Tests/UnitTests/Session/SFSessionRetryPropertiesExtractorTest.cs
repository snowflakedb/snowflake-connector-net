using System;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session;

public sealed class SFSessionRetryPropertiesExtractorTest
{
    [SFFact]
    public void TestExtractsDefaultRetryProperties()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.Equal(TimeSpan.FromSeconds(300), result.ConnectionTimeout);
        Assert.False(result.DisableRetry);
        Assert.False(result.ForceRetryOn404);
        Assert.Equal(TimeSpan.FromSeconds(300), result.RetryTimeout);
        Assert.Equal(7, result.MaxHttpRetries);
        Assert.True(result.IncludeRetryReason);
    }

    [SFFact]
    public void TestExtractsCustomConnectionTimeout()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;CONNECTION_TIMEOUT=60");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.Equal(TimeSpan.FromSeconds(60), result.ConnectionTimeout);
    }

    [SFFact]
    public void TestExtractsDisableRetryTrue()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;DISABLERETRY=true");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.True(result.DisableRetry);
    }

    [SFFact]
    public void TestExtractsForceRetryOn404True()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;FORCERETRYON404=true");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.True(result.ForceRetryOn404);
    }

    [SFFact]
    public void TestExtractsCustomRetryTimeout()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;RETRY_TIMEOUT=600");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.Equal(TimeSpan.FromSeconds(600), result.RetryTimeout);
    }

    [SFFact]
    public void TestExtractsNegativeRetryTimeoutAsInfinity()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;RETRY_TIMEOUT=-1");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.Equal(TimeoutHelper.Infinity(), result.RetryTimeout);
    }

    [SFFact]
    public void TestExtractsCustomMaxHttpRetries()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;MAXHTTPRETRIES=10");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.Equal(10, result.MaxHttpRetries);
    }

    [SFFact]
    public void TestExtractsIncludeRetryReasonFalse()
    {
        // arrange
        var props = ParseConnectionString("ACCOUNT=account;USER=test;PASSWORD=test;INCLUDERETRYREASON=false");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.False(result.IncludeRetryReason);
    }

    [SFFact]
    public void TestExtractsAllCustomRetryProperties()
    {
        // arrange
        var props = ParseConnectionString(
            "ACCOUNT=account;USER=test;PASSWORD=test;" +
            "CONNECTION_TIMEOUT=60;DISABLERETRY=true;FORCERETRYON404=true;" +
            "RETRY_TIMEOUT=600;MAXHTTPRETRIES=15;INCLUDERETRYREASON=false");
        var extractor = new SFSessionRetryProperties.Extractor();
        var defaultValuesExtractor = new SessionPropertiesWithDefaultValuesExtractor(props, true);

        // act
        var result = extractor.ExtractProperties(props, defaultValuesExtractor);

        // assert
        Assert.Equal(TimeSpan.FromSeconds(60), result.ConnectionTimeout);
        Assert.True(result.DisableRetry);
        Assert.True(result.ForceRetryOn404);
        Assert.Equal(TimeSpan.FromSeconds(600), result.RetryTimeout);
        Assert.Equal(15, result.MaxHttpRetries);
        Assert.False(result.IncludeRetryReason);
    }

    private static SFSessionProperties ParseConnectionString(string connectionString)
        => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());
}
