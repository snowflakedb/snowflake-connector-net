namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Client;
    sealed class SFDbCommandBuilderTest
    {
        SnowflakeDbCommandBuilder builder;

        public SFDbCommandBuilderTest()
        {
            builder = new SnowflakeDbCommandBuilder();
        }

        [Fact]
        public void TestDefaultCommandBuilder()
        {
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
        }

        [Fact]
        public void TestCommandBuilderWithoutAdapter()
        {
            builder = new SnowflakeDbCommandBuilder();
            Assert.Equal(null, builder.DataAdapter);
        }

        [Fact]
        public void TestCommandBuilderWithAdapter()
        {
            SnowflakeDbDataAdapter adapter = new SnowflakeDbDataAdapter();
            builder = new SnowflakeDbCommandBuilder(adapter);
            Assert.Equal(adapter, builder.DataAdapter);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
        }

        [Fact]
        public void TestCommandBuilderSetPrefix()
        {
            string newQuotePrefix = "\'";

            Assert.NotEqual(newQuotePrefix, builder.QuotePrefix);

            builder.QuotePrefix = newQuotePrefix;
            Assert.Equal(newQuotePrefix, builder.QuotePrefix);
        }

        [Fact]
        public void TestCommandBuilderSetSuffix()
        {
            string newQuoteSuffix = "\'";

            Assert.NotEqual(newQuoteSuffix, builder.QuoteSuffix);

            builder.QuoteSuffix = newQuoteSuffix;
            Assert.Equal(newQuoteSuffix, builder.QuoteSuffix);
        }
    }
}
