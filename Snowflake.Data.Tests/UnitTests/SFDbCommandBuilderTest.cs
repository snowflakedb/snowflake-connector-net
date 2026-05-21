namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Client;


    class SFDbCommandBuilderTest
    {
        SnowflakeDbCommandBuilder builder;

        [SetUp]
        public void BeforeTest()
        {
            builder = new SnowflakeDbCommandBuilder();
        }

        [Test]
        public void TestDefaultCommandBuilder()
        {
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
        }

        [Test]
        public void TestCommandBuilderWithoutAdapter()
        {
            builder = new SnowflakeDbCommandBuilder();
            Assert.Equal(null, builder.DataAdapter);
        }

        [Test]
        public void TestCommandBuilderWithAdapter()
        {
            SnowflakeDbDataAdapter adapter = new SnowflakeDbDataAdapter();
            builder = new SnowflakeDbCommandBuilder(adapter);
            Assert.Equal(adapter, builder.DataAdapter);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.Equal(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
        }

        [Test]
        public void TestCommandBuilderSetPrefix()
        {
            string newQuotePrefix = "\'";

            Assert.NotEqual(newQuotePrefix, builder.QuotePrefix);

            builder.QuotePrefix = newQuotePrefix;
            Assert.Equal(newQuotePrefix, builder.QuotePrefix);
        }

        [Test]
        public void TestCommandBuilderSetSuffix()
        {
            string newQuoteSuffix = "\'";

            Assert.NotEqual(newQuoteSuffix, builder.QuoteSuffix);

            builder.QuoteSuffix = newQuoteSuffix;
            Assert.Equal(newQuoteSuffix, builder.QuoteSuffix);
        }
    }
}
