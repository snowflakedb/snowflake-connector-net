using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Session;

public sealed class QueryLoggingConfigTest
{
    private const string BaseConnectionString = "ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;";

    [SFFact]
    public void TestParseExplicitValues()
    {
        var connectionString = $"{BaseConnectionString}LOG_QUERY_TEXT=true;LOG_QUERY_PARAMETERS=true;LOG_MAX_QUERY_LENGTH=500;";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        Assert.Equal("true", properties[SFSessionProperty.LOG_QUERY_TEXT]);
        Assert.Equal("true", properties[SFSessionProperty.LOG_QUERY_PARAMETERS]);
        Assert.Equal("500", properties[SFSessionProperty.LOG_MAX_QUERY_LENGTH]);
    }

    [SFFact]
    public void TestExtractWithDefaults()
    {
        var logger = new Mock<SFLogger>();
        var properties = SFSessionProperties.ParseConnectionString(BaseConnectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out var config);

        Assert.False(config.LogQueryText);
        Assert.False(config.LogQueryParameters);
        Assert.Equal(-1, config.MaxQueryLength);
    }

    [SFFact]
    public void TestExtractWithExplicitValues()
    {
        var logger = new Mock<SFLogger>();
        var connectionString = $"{BaseConnectionString}LOG_QUERY_TEXT=true;LOG_QUERY_PARAMETERS=true;LOG_MAX_QUERY_LENGTH=1000;";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out var config);

        Assert.True(config.LogQueryText);
        Assert.True(config.LogQueryParameters);
        Assert.Equal(1000, config.MaxQueryLength);
    }

    [SFTheory]
    [InlineData("0")]
    [InlineData("-5")]
    [InlineData("-2")]
    [InlineData("1")]
    [InlineData("49")]
    public void TestExtractThrowsForInvalidMaxQueryLength(string invalidValue)
    {
        var connectionString = $"{BaseConnectionString}LOG_MAX_QUERY_LENGTH={invalidValue};";

        var exception = Assert.Throws<SnowflakeDbException>(() =>
            SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));
        Assert.Contains("LOG_MAX_QUERY_LENGTH", exception.Message);
    }

    [SFTheory]
    [InlineData("banana")]
    [InlineData("abc")]
    [InlineData("1.5")]
    public void TestExtractThrowsForNonIntegerMaxQueryLength(string invalidValue)
    {
        var connectionString = $"{BaseConnectionString}LOG_MAX_QUERY_LENGTH={invalidValue};";

        var exception = Assert.Throws<SnowflakeDbException>(() =>
            SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));
        Assert.Contains("LOG_MAX_QUERY_LENGTH", exception.Message);
    }

    [SFFact]
    public void TestExtractAcceptsMinusOneAsNoTruncation()
    {
        var logger = new Mock<SFLogger>();
        var connectionString = $"{BaseConnectionString}LOG_MAX_QUERY_LENGTH=-1;";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out var config);

        Assert.Equal(-1, config.MaxQueryLength);
    }

    [Collection(nameof(IsolatedFixture))]
    public sealed class Isolated
    {
        [CollectionDefinition(nameof(IsolatedFixture), DisableParallelization = true)]
        public sealed class IsolatedFixture : ICollectionFixture<IsolatedFixture>;

        [SFFact]
        public void TestStaticDefaultsAreUsedWhenNotInConnectionString()
        {
            SFLoggerFactory.SetDefaultLogQueryText(true);
            SFLoggerFactory.SetDefaultLogQueryParameters(true);
            SFLoggerFactory.SetDefaultLogMaxQueryLength(2000);
            try
            {
                var logger = new Mock<SFLogger>();
                var properties = SFSessionProperties.ParseConnectionString(BaseConnectionString, new SessionPropertiesContext());

                // Remove property defaults so ValidateAndExtract falls back to static defaults
                properties.Remove(SFSessionProperty.LOG_QUERY_TEXT);
                properties.Remove(SFSessionProperty.LOG_QUERY_PARAMETERS);
                properties.Remove(SFSessionProperty.LOG_MAX_QUERY_LENGTH);

                QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out var config);

                Assert.True(config.LogQueryText);
                Assert.True(config.LogQueryParameters);
                Assert.Equal(2000, config.MaxQueryLength);
            }
            finally
            {
                SFLoggerFactory.SetDefaultLogQueryText(false);
                SFLoggerFactory.SetDefaultLogQueryParameters(false);
                SFLoggerFactory.SetDefaultLogMaxQueryLength(-1);
            }
        }

        [SFFact]
        public void TestConnectionStringOverridesStaticDefaults()
        {
            SFLoggerFactory.SetDefaultLogQueryText(true);
            try
            {
                var logger = new Mock<SFLogger>();
                var connectionString = $"{BaseConnectionString}LOG_QUERY_TEXT=false;";
                var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

                QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out var config);

                Assert.False(config.LogQueryText);
            }
            finally
            {
                SFLoggerFactory.SetDefaultLogQueryText(false);
            }
        }
    }

    [SFFact]
    public void TestWarnAdvisoryWhenQueryTextEnabled()
    {
        var logger = new Mock<SFLogger>();
        var connectionString = $"{BaseConnectionString}LOG_QUERY_TEXT=true;";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out _);

        logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains("LOG_QUERY_TEXT is enabled")), null), Times.Once);
    }

    [SFFact]
    public void TestWarnAdvisoryWhenQueryParametersEnabled()
    {
        var logger = new Mock<SFLogger>();
        var connectionString = $"{BaseConnectionString}LOG_QUERY_TEXT=true;LOG_QUERY_PARAMETERS=true;";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out _);

        logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains("LOG_QUERY_PARAMETERS is enabled")), null), Times.Once);
    }

    [SFFact]
    public void TestWarnAdvisoryWhenMaxLengthSetButQueryTextDisabled()
    {
        var logger = new Mock<SFLogger>();
        var connectionString = $"{BaseConnectionString}LOG_MAX_QUERY_LENGTH=500;";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out _);

        logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains("LOG_MAX_QUERY_LENGTH is set but LOG_QUERY_TEXT is disabled")), null), Times.Once);
    }

    [SFFact]
    public void TestNoWarningsWhenAllDisabled()
    {
        var logger = new Mock<SFLogger>();
        var properties = SFSessionProperties.ParseConnectionString(BaseConnectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out _);

        logger.Verify(l => l.Warn(It.IsAny<string>(), null), Times.Never);
    }

    [SFFact]
    public void TestLogQueryParametersIgnoredWhenQueryTextDisabled()
    {
        var logger = new Mock<SFLogger>();
        var connectionString = $"{BaseConnectionString}LOG_QUERY_PARAMETERS=true;";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out var config);

        Assert.False(config.LogQueryParameters);
        logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains("LOG_QUERY_PARAMETERS is set but LOG_QUERY_TEXT is disabled")), null), Times.Once);
    }

    [SFFact]
    public void TestLogQueryParametersEnabledWhenQueryTextAlsoEnabled()
    {
        var logger = new Mock<SFLogger>();
        var connectionString = $"{BaseConnectionString}LOG_QUERY_TEXT=true;LOG_QUERY_PARAMETERS=true;";
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        QueryLoggingConfig.ValidateAndExtract(properties, logger.Object, out var config);

        Assert.True(config.LogQueryParameters);
    }
}
