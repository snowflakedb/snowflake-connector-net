using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Client;
    public sealed class SFDbCommandBuilderTest
    {
        SnowflakeDbCommandBuilder builder;

        public SFDbCommandBuilderTest()
        {
            builder = new SnowflakeDbCommandBuilder();
        }

        [SFFact]
        public void TestDefaultCommandBuilder()
        {
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
        }

        [SFFact]
        public void TestCommandBuilderWithoutAdapter()
        {
            builder = new SnowflakeDbCommandBuilder();
            Assert.Null(builder.DataAdapter);
        }

        [SFFact]
        public void TestCommandBuilderWithAdapter()
        {
            SnowflakeDbDataAdapter adapter = new SnowflakeDbDataAdapter();
            builder = new SnowflakeDbCommandBuilder(adapter);
            Assert.Equal(adapter, builder.DataAdapter);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
        }

        [SFFact]
        public void TestCommandBuilderSetPrefix()
        {
            string newQuotePrefix = "\'";

            Assert.NotEqual(newQuotePrefix, builder.QuotePrefix);

            builder.QuotePrefix = newQuotePrefix;
            Assert.Equal(newQuotePrefix, builder.QuotePrefix);
        }

        [SFFact]
        public void TestCommandBuilderSetSuffix()
        {
            string newQuoteSuffix = "\'";

            Assert.NotEqual(newQuoteSuffix, builder.QuoteSuffix);

            builder.QuoteSuffix = newQuoteSuffix;
            Assert.Equal(newQuoteSuffix, builder.QuoteSuffix);
        }
    }
}
