using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests;

using Xunit;
using Snowflake.Data.Client;
public sealed class SFDbCommandBuilderTest
{
    private SnowflakeDbCommandBuilder _builder;

    public SFDbCommandBuilderTest()
    {
        _builder = new SnowflakeDbCommandBuilder();
    }

    [SFFact]
    public void TestDefaultCommandBuilder()
    {
        Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, _builder.QuotePrefix);
        Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, _builder.QuoteSuffix);
    }

    [SFFact]
    public void TestCommandBuilderWithoutAdapter()
    {
        _builder = new SnowflakeDbCommandBuilder();
        Assert.Null(_builder.DataAdapter);
    }

    [SFFact]
    public void TestCommandBuilderWithAdapter()
    {
        var adapter = new SnowflakeDbDataAdapter();
        _builder = new SnowflakeDbCommandBuilder(adapter);
        Assert.Equal(adapter, _builder.DataAdapter);
        Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, _builder.QuotePrefix);
        Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, _builder.QuoteSuffix);
    }

    [SFFact]
    public void TestCommandBuilderSetPrefix()
    {
        var newQuotePrefix = "\'";

        Assert.NotEqual(newQuotePrefix, _builder.QuotePrefix);

        _builder.QuotePrefix = newQuotePrefix;
        Assert.Equal(newQuotePrefix, _builder.QuotePrefix);
    }

    [SFFact]
    public void TestCommandBuilderSetSuffix()
    {
        var newQuoteSuffix = "\'";

        Assert.NotEqual(newQuoteSuffix, _builder.QuoteSuffix);

        _builder.QuoteSuffix = newQuoteSuffix;
        Assert.Equal(newQuoteSuffix, _builder.QuoteSuffix);
    }
}
