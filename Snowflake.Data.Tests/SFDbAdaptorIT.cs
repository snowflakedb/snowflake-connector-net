/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;

    [TestFixture]
    class SFDbAdaptorIT : SFBaseTest
    {
        [Test]
        public void TestSelectStatement()
        {
            DataSet ds = new DataSet("ds");
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbDataAdapter adaptor = new SnowflakeDbDataAdapter("select 1 as col1, 2 AS col2", conn);
                adaptor.Fill(ds);
                conn.Close();
            }
            Assert.AreEqual(ds.Tables[0].Rows[0]["col1"].ToString(), "1");
            Assert.AreEqual(ds.Tables[0].Rows[0]["col2"].ToString(), "2");
        }

        [Test]
        public void TestSelectTimeout()
        {
            DataSet ds = new DataSet("ds");
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                try
                {
                    IDbDataAdapter adaptor = new SnowflakeDbDataAdapter("select count(seq4()) from table(generator(timelimit => 60))", conn);
                    adaptor.SelectCommand.CommandTimeout = 1;
                    adaptor.Fill(ds);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(e.ErrorCode, 604);
                }
            }
        }
    }
}
