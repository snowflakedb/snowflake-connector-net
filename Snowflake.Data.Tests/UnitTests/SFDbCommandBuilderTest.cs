namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;

    [TestFixture]
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
            Assert.AreEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.AreEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
        }

        [Test]
        public void TestCommandBuilderWithoutAdapter()
        {
            builder = new SnowflakeDbCommandBuilder();
            Assert.AreEqual(null, builder.DataAdapter);
        }

        [Test]
        public void TestCommandBuilderWithAdapter()
        {
            SnowflakeDbDataAdapter adapter = new SnowflakeDbDataAdapter();
            builder = new SnowflakeDbCommandBuilder(adapter);
            Assert.AreEqual(adapter, builder.DataAdapter);
            Assert.AreEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.AreEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
        }

        [Test]
        public void TestCommandBuilderSetPrefix()
        {
            string newQuotePrefix = "\'";

            Assert.AreNotEqual(newQuotePrefix, builder.QuotePrefix);

            builder.QuotePrefix = newQuotePrefix;
            Assert.AreEqual(newQuotePrefix, builder.QuotePrefix);
        }

        [Test]
        public void TestCommandBuilderSetSuffix()
        {
            string newQuoteSuffix = "\'";

            Assert.AreNotEqual(newQuoteSuffix, builder.QuoteSuffix);

            builder.QuoteSuffix = newQuoteSuffix;
            Assert.AreEqual(newQuoteSuffix, builder.QuoteSuffix);
        }
    }
}
