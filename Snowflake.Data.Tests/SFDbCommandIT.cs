/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System.Data;
using System.Data.Common;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Configuration;
    using System.Diagnostics;

    [TestFixture]
    class SFDbCommandITAsync : SFBaseTestAsync
    {

        [Test]
        public void TestExecAsyncAPI()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                Task connectTask = conn.OpenAsync(CancellationToken.None);
                Assert.AreEqual(ConnectionState.Connecting, conn.State);

                connectTask.Wait();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                using (DbCommand cmd = conn.CreateCommand())
                {
                    int queryResult = 0;
                    cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 3)) v";
                    Task<DbDataReader> execution = cmd.ExecuteReaderAsync();
                    Task readCallback = execution.ContinueWith((t) =>
                    {
                        using (DbDataReader reader = t.Result)
                        {
                            Assert.IsTrue(reader.Read());
                            queryResult = reader.GetInt32(0);
                            Assert.IsFalse(reader.Read());
                        }
                    });
                    // query is not finished yet, result is still 0;
                    Assert.AreEqual(0, queryResult);
                    // block till query finished
                    readCallback.Wait();
                    // queryResult should be updated by callback
                    Assert.AreNotEqual(0, queryResult);
                }

                conn.Close();
            }
        }

        [Test]
        public void TestCancelExecuteAsync()
        {
            CancellationTokenSource externalCancel = new CancellationTokenSource(TimeSpan.FromSeconds(8));

            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 20)) v";
                // external cancellation should be triggered before timeout
                cmd.CommandTimeout = 10;
                try
                {
                    Task<object> t = cmd.ExecuteScalarAsync(externalCancel.Token);
                    t.Wait();
                    Assert.Fail();
                }
                catch
                {
                    // assert that cancel is not triggered by timeout, but external cancellation 
                    Assert.IsTrue(externalCancel.IsCancellationRequested);
                }
                Thread.Sleep(2000);
                conn.Close();
            }
        }

    }

    [TestFixture]
    class SFDbCommandITSlow : SFBaseTest
    {

        [Test]
        public void TestLongRunningQuery()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 60)) v order by 1";
                IDataReader reader = cmd.ExecuteReader();
                // only one result is returned
                Assert.IsTrue(reader.Read());

                conn.Close();
            }

        }

        [Test]
        public void TestRowsAffectedOverflowInt()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "create or replace table test_rows_affected_overflow(c1 number)";
                    command.ExecuteNonQuery();

                    command.CommandText = "insert into test_rows_affected_overflow select seq4() from table(generator(rowcount=>2147484000))";
                    int affected = command.ExecuteNonQuery();

                    Assert.AreEqual(-1, affected);

                    command.CommandText = "drop table if exists test_rows_affected_overflow";
                    command.ExecuteNonQuery();
                }
                conn.Close();
            }
        }

    }

    [TestFixture]    
    class SFDbCommandIT : SFBaseTest
    {
        [Test]
        public void TestSimpleCommand()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select 1";

                // command type can only be text, stored procedure are not supported.
                Assert.AreEqual(CommandType.Text, cmd.CommandType);
                try
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                Assert.AreEqual(UpdateRowSource.None, cmd.UpdatedRowSource);
                try
                {
                    cmd.UpdatedRowSource = UpdateRowSource.FirstReturnedRecord;
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                Assert.AreSame(conn, cmd.Connection);
                try
                {
                    cmd.Connection = null;
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                Assert.IsFalse(((SnowflakeDbCommand)cmd).DesignTimeVisible);
                try
                {
                    ((SnowflakeDbCommand)cmd).DesignTimeVisible = true;
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                object val = cmd.ExecuteScalar();
                Assert.AreEqual(1L, (long)val);

                conn.Close();
            }
        }

        [Test]
        // Skip SimpleLargeResultSet test on GCP as it will fail
        // on row 8192 consistently on Appveyor.
        [IgnoreOnEnvIs("snowflake_cloud_env",
                       new string[] {"GCP" })]
        public void TestSimpleLargeResultSet()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 1000000)) v order by 1";
                IDataReader reader = cmd.ExecuteReader();
                int counter = 0;
                while (reader.Read())
                {
                    Assert.AreEqual(counter.ToString(), reader.GetString(0));
                    counter++;
                }
                conn.Close();
            }
        }


        /*
         * Disabled to make sure that configuration changes does not cause problems with appveyor
         * 
        [Test]
        public void TestUseV1ResultParser()
        {
            SFConfiguration.Instance().UseV2JsonParser = false;

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 200000)) v order by 1";
                IDataReader reader = cmd.ExecuteReader();
                int counter = 0;
                while (reader.Read())
                {
                    Assert.AreEqual(counter.ToString(), reader.GetString(0));
                    counter++;
                }
                conn.Close();
            }
            SFConfiguration.Instance().UseV2JsonParser = true;
        }

        [Test]
        public void TestUseV2ChunkDownloader()
        {
            SFConfiguration.Instance().UseV2ChunkDownloader = true;

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 200000)) v order by 1";
                IDataReader reader = cmd.ExecuteReader();
                int counter = 0;
                while (reader.Read())
                {
                    Assert.AreEqual(counter.ToString(), reader.GetString(0));
                    counter++;
                }
                conn.Close();
            }
            SFConfiguration.Instance().UseV2ChunkDownloader = false;
        }
        */


        [Test]
        public void TestDataSourceError()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select * from table_not_exists";
                try
                {
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(2003, e.ErrorCode);
                    Assert.AreNotEqual("", e.QueryId);
                }

                conn.Close();
            }
        }

        [Test]
        public void TestCancelQuery()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 20)) v";
                Task executionThread = Task.Run(() =>
                {
                    try
                    {
                        cmd.ExecuteScalar();
                        Assert.Fail();
                    }
                    catch(SnowflakeDbException e)
                    {
                        // 604 is error code from server meaning query has been canceled
                        if (604 != e.ErrorCode)
                        {
                            Assert.Fail($"Unexpected error code {e.ErrorCode} for {e.Message}");
                        }
                    }
                });

                Thread.Sleep(8000);
                cmd.Cancel();

                try
                {
                    executionThread.Wait();
                }
                catch (AggregateException e)
                {
                    if (e.InnerException.GetType() != typeof(NUnit.Framework.AssertionException))
                    {
                        Assert.AreEqual(
                        "System.Threading.Tasks.TaskCanceledException",
                        e.InnerException.GetType().ToString());
                    }
                    else
                    {
                        // Unexpected exception
                        throw;
                    }
                }

                conn.Close();
            }
        }

        [Test]
        public void TestQueryTimeout()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                // timelimit = 17min
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 1020)) v";
                // timeout = 16min - Using a timeout > default Rest timeout of 15min
                cmd.CommandTimeout = 16*60; 

                try
                {
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    cmd.ExecuteScalar();
                    stopwatch.Stop();
                    //Should timeout before the query time limit of 17min
                    Assert.Less(stopwatch.ElapsedMilliseconds, 17 * 60 * 1000);
                    // Should timeout after the defined query timeout of 16min
                    Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 16 * 60 * 1000);
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    // 604 is error code from server meaning query has been canceled
                    Assert.AreEqual(e.ErrorCode, 604);
                }

                conn.Close();
            }

        }

        [Test]
        public void TestTransaction()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                try
                {
                    conn.BeginTransaction(IsolationLevel.ReadUncommitted);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                IDbTransaction tran = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                IDbCommand command = conn.CreateCommand();
                command.Transaction = tran;
                command.CommandText = "create or replace table testtransaction(cola string)";
                command.ExecuteNonQuery();
                command.Transaction.Commit();

                command.CommandText = "show tables like 'testtransaction'";
                IDataReader reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());
                Assert.IsFalse(reader.Read());

                // start another transaction to test rollback
                tran = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                command.Transaction = tran;
                command.CommandText = "insert into testtransaction values('test')";

                command.ExecuteNonQuery();
                command.CommandText = "select * from testtransaction";
                reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("test", reader.GetString(0));
                command.Transaction.Rollback();
                
                // no value will be in table since it has been rollbacked
                command.CommandText = "select * from testtransaction";
                reader = command.ExecuteReader();
                Assert.IsFalse(reader.Read());

                conn.Close();
            }
        }

        [Test]
        public void TestRowsAffected()
        {
            String[] testCommands =
            {
                "create or replace table test_rows_affected(cola int, colb string)",
                "insert into test_rows_affected values(1, 'a'),(2, 'b')",
                "merge into test_rows_affected using (select 1 as cola, 'c' as colb) m on " +
                "test_rows_affected.cola = m.cola when matched then update set test_rows_affected.colb='update' " +
                "when not matched then insert (cola, colb) values (3, 'd')",
                "drop table if exists test_rows_affected"
            };

            int[] expectedResult =
            {
                0, 2, 1, 0 
            };

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                conn.Open();

                using (IDbCommand command = conn.CreateCommand())
                {
                    int rowsAffected = -1;
                    for (int i=0; i<testCommands.Length; i++)
                    {
                        command.CommandText = testCommands[i];
                        rowsAffected = command.ExecuteNonQuery();

                        Assert.AreEqual(expectedResult[i], rowsAffected);
                    }
                }
                conn.Close();
            }    
        }

        [Test]
        public void TestExecuteScalarNull()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "select 1 where 2 > 3";
                    object val = command.ExecuteScalar();

                    Assert.AreEqual(DBNull.Value, val);
                }
                conn.Close();
            }
        }

        [Test]
        public void TestCreateCommandBeforeOpeningConnection()
        {
            using(var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                
                using(var command = conn.CreateCommand())
                {
                    conn.Open();
                    command.CommandText = "select 1";
                    Assert.DoesNotThrow(() => command.ExecuteNonQuery());
                }
            }
        }
    }
}
