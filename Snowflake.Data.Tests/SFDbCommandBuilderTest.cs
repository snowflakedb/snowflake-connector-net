/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;

    [TestFixture]
    class SFDbCommandBuilderTest : SFBaseTest
    {
        SnowflakeDbCommandBuilder builder;

        [SetUp]
        public void BeforeTest()
        {
            builder = new SnowflakeDbCommandBuilder();
        }

        [Test]
        [Ignore("DbCommandBuilder")]
        public void DbCommandBuilderDone()
        {
            // Do nothing;
        }

        [Test]
        public void TestEmptyCommandBuilder()
        {
            Assert.AreEqual(null, builder.DataAdapter);
            Assert.AreEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.AreEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
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

            Assert.AreEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.AreNotEqual(newQuotePrefix, builder.QuotePrefix);

            builder.QuotePrefix = newQuotePrefix;
            Assert.AreNotEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_PREFIX, builder.QuotePrefix);
            Assert.AreEqual(newQuotePrefix, builder.QuotePrefix);
        }

        [Test]
        public void TestCommandBuilderSetSuffix()
        {
            string newQuoteSuffix = "\'";

            Assert.AreEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
            Assert.AreNotEqual(newQuoteSuffix, builder.QuoteSuffix);

            builder.QuoteSuffix = newQuoteSuffix;
            Assert.AreNotEqual(SnowflakeDbCommandBuilder.DEFAULT_QUOTE_SUFFIX, builder.QuoteSuffix);
            Assert.AreEqual(newQuoteSuffix, builder.QuoteSuffix);
        }
    }
}
