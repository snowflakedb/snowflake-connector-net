/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using System.Data;
    using System.Data.Common;
    using System;
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using System.Threading.Tasks;

    [TestFixture]
    class SFDbTransactionIT : SFBaseTest
    {
        [Test]
        [Ignore("DbTransactionIT")]
        public void DbTransactionITDone()
        {
            // Do nothing;
        }

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

        [Test]
        // Test SNOW-761136 unnecessary ROLLBACK 
        public void TestTransactionRollback()
        {
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString;
            //Task connectTask = conn.OpenAsync();
            //connectTask.Wait();
            conn.Open();

            DbCommand syncPrep = conn.CreateCommand();
            syncPrep.CommandText = "create or replace table test_async(x timestamp, a integer)";
            syncPrep.ExecuteNonQuery();
            Console.WriteLine("Created table");

            syncPrep.CommandText = "insert into test_async values (current_timestamp(), 1), (current_timestamp(), 2), (current_timestamp(), 3)";
            Console.WriteLine("Inserted into table; num rows: " + syncPrep.ExecuteNonQuery());

            using (DbTransaction transaction = conn.BeginTransaction())
            {
                try
                {
                    DbCommand command = conn.CreateCommand();
                    command.Transaction = transaction;
                    Console.WriteLine("Running ~60 updates");
                    for (int i = 0; i < 30; i++)
                    {
                        command.CommandText = "UPDATE TEST_ASYNC SET x = current_timestamp(), a = 6 WHERE a = 3";
                        command.ExecuteNonQuery();
                        command.CommandText = "UPDATE TEST_ASYNC SET x = current_timestamp(), a = 3 WHERE a = 6";
                        command.ExecuteNonQuery();
                    }

                    Console.WriteLine("Launching a race between COMMIT and ROLLBACK");
                    DbCommand asyncBlockTwo = conn.CreateCommand();
                    asyncBlockTwo.Transaction = transaction;
                    asyncBlockTwo.CommandText = "COMMIT";
                    DbCommand asyncBlockThree = conn.CreateCommand();
                    asyncBlockThree.Transaction = transaction;
                    asyncBlockThree.CommandText = "ROLLBACK";
                    Task<int> asyncBlockTwoTask = asyncBlockTwo.ExecuteNonQueryAsync();
                    Task<int> asyncBlockThreeTask = asyncBlockThree.ExecuteNonQueryAsync();

                    asyncBlockTwoTask.Wait();
                    asyncBlockThreeTask.Wait();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    Console.WriteLine(ex);
                }
            }

            Console.WriteLine("Checking table contents...");
            DbCommand syncReader = conn.CreateCommand();
            syncReader.CommandText = "select x, a from test_async";
            DbDataReader reader = syncReader.ExecuteReader();
            while (reader.Read())
            {
                Console.Write("Row (col a): " + reader.GetInt64(1) + " ");
                Console.WriteLine("(col x): " + reader.GetDateTime(0));
            }

            conn.Close();
        }
    }
}
