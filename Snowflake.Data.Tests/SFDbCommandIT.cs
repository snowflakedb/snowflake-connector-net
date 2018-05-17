/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
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
    using Snowflake.Data.Core;

    [TestFixture]    
    class SFDbCommandIT : SFBaseTest
    {
        [Test]
        public void testSimpleCommand()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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
        public void testSimpleLargeResultSet()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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
        }

        [Test]
        public void testLongRunningQuery()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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
        public void testDataSourceError()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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
                    Assert.AreNotEqual("", e.queryId);
                }

                conn.Close();
            }
        }

        [Test]
        public void testCancelQuery()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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
                        Assert.AreEqual(e.ErrorCode, 604);
                    }
                });

                Thread.Sleep(5000);
                cmd.Cancel();

                executionThread.Wait();

                conn.Close();
            }
        }

        [Test]
        public void testQueryTimeout()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 20)) v";
                cmd.CommandTimeout = 10;

                try
                {
                    cmd.ExecuteScalar();
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    // 604 is error code from server meaning query has been cancelled
                    Assert.AreEqual(e.ErrorCode, 604);
                }

                conn.Close();
            }

        }

        [Test]
        public void testTransaction()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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
        public void testRowsAffected()
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
                conn.ConnectionString = connectionString;

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
        public void testExecAsyncAPI()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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

        //[Test]
        public void testCancelExecuteAsync()
        {
            CancellationTokenSource externalCancel = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

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
                catch(AggregateException e)
                {
                    // assert that cancel is not triggered by timeout, but external cancellation 
                    Assert.IsTrue(externalCancel.IsCancellationRequested);
                }
                conn.Close();
            }

        }
    }
}
