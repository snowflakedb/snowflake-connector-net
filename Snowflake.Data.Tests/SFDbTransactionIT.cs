/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;

    [TestFixture]
    class SFDbTransactionIT : SFBaseTest
    {
        [Test]
        // Test that when a transaction is disposed, rollback would be sent out
        public void TestTransactionDispose()
        {
            var conn = new SnowflakeDbConnection();
            try
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand command = conn.CreateCommand();
                command.CommandText = "create or replace table testTransactionDispose(c int)";
                command.ExecuteNonQuery();

                using (IDbTransaction t1 = conn.BeginTransaction())
                {
                    IDbCommand t1c1 = conn.CreateCommand();
                    t1c1.Transaction = t1;
                    t1c1.CommandText = "insert into testTransactionDispose values (1)";
                    t1c1.ExecuteNonQuery();
                }

                // Transaction t1 would be disposed and rollback at this point, tuple inserted is not visible
                IDbCommand c2 = conn.CreateCommand();
                c2.CommandText = "SELECT * FROM testTransactionDispose";
                IDataReader reader2 = c2.ExecuteReader();
                Assert.IsFalse(reader2.Read());
            }
            finally
            {
                IDbCommand command = conn.CreateCommand();
                command.CommandText = "DROP TABLE IF EXISTS testTransactionDispose";
                command.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}
