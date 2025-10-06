using System.Data;
using System.Data.Common;
using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using System.Linq;
using System.IO;

namespace Snowflake.Data.Tests.IntegrationTests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Configuration;
    using System.Diagnostics;
    using System.Collections.Generic;
    using System.Globalization;
    using Snowflake.Data.Tests.Mock;

    [TestFixture]
    class SFDbCommandITAsync : SFBaseTestAsync
    {
        [Test]
        public void TestExecAsyncAPI()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                using (DbCommand cmd = conn.CreateCommand())
                {
                    long queryResult = 0;
                    cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 3)) v";
                    Task<DbDataReader> execution = cmd.ExecuteReaderAsync();
                    Task readCallback = execution.ContinueWith((t) =>
                    {
                        using (DbDataReader reader = t.Result)
                        {
                            Assert.IsTrue(reader.Read());
                            queryResult = reader.GetInt64(0);
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
        public void TestExecAsyncAPIParallel()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                Task[] taskArray = new Task[5];
                for (int i = 0; i < taskArray.Length; i++)
                {
                    taskArray[i] = Task.Factory.StartNew(() =>
                    {
                        using (DbCommand cmd = conn.CreateCommand())
                        {
                            long queryResult = 0;
                            cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 3)) v";
                            Task<DbDataReader> execution = cmd.ExecuteReaderAsync();
                            Task readCallback = execution.ContinueWith((t) =>
                            {
                                using (DbDataReader reader = t.Result)
                                {
                                    Assert.IsTrue(reader.Read());
                                    queryResult = reader.GetInt64(0);
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
                    });
                }
                Task.WaitAll(taskArray);
                conn.Close();
            }
        }

        [Test]
        public void TestCancelExecuteAsync()
        {
            CancellationTokenSource externalCancel = new CancellationTokenSource(TimeSpan.FromSeconds(8));

            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

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

        [Test]
        public void TestExecuteAsyncWithMaxRetryReached()
        {
            var mockRestRequester = new MockRetryUntilRestTimeoutRestRequester()
            {
                _forceTimeoutForNonLoginRequestsOnly = true
            };

            using (DbConnection conn = new MockSnowflakeDbConnection(mockRestRequester))
            {
                string maxRetryConnStr = ConnectionString + "maxHttpRetries=8;poolingEnabled=false";

                conn.ConnectionString = maxRetryConnStr;
                conn.Open();

                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    using (DbCommand command = conn.CreateCommand())
                    {
                        command.CommandText = "select 1;";
                        Task<object> t = command.ExecuteScalarAsync();
                        t.Wait();
                    }
                    Assert.Fail();
                }
                catch (Exception e)
                {
                    Assert.IsInstanceOf<TaskCanceledException>(e.InnerException);
                }
                stopwatch.Stop();

                var totalDelaySeconds = 1 + 2 + 4 + 8 + 16 + 16 + 16 + 16;
                // retry 8 times with backoff 1, 2, 4, 8, 16, 16, 16, 16 seconds
                // but should not delay more than another 16 seconds
                Assert.Less(stopwatch.ElapsedMilliseconds, (totalDelaySeconds + 20) * 1000);
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, totalDelaySeconds * 1000);
            }
        }

        [Test]
        public async Task TestAsyncExecQueryAsync()
        {
            string queryId;
            var expectedWaitTime = 5;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    cmd.CommandText = $"CALL SYSTEM$WAIT({expectedWaitTime}, \'SECONDS\');";

                    // Act
                    queryId = await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);
                    var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);

                    // Assert
                    Assert.IsTrue(conn.IsStillRunning(queryStatus));
                    Assert.IsFalse(conn.IsAnError(queryStatus));

                    // Act
                    DbDataReader reader = await cmd.GetResultsFromQueryIdAsync(queryId, CancellationToken.None).ConfigureAwait(false);
                    queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);

                    // Assert
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual($"waited {expectedWaitTime} seconds", reader.GetString(0));
                    Assert.AreEqual(QueryStatus.Success, queryStatus);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [Test, NonParallelizable]
        public async Task TestExecuteNormalQueryWhileAsyncExecQueryIsRunningAsync()
        {
            string queryId;
            var expectedWaitTime = 5;

            SnowflakeDbConnection[] connections = new SnowflakeDbConnection[3];
            for (int i = 0; i < connections.Length; i++)
            {
                connections[i] = new SnowflakeDbConnection(ConnectionString + "poolingEnabled=false");
                await connections[i].OpenAsync(CancellationToken.None).ConfigureAwait(false);
            }

            // Start the async exec query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[0].CreateCommand())
            {
                // Arrange
                cmd.CommandText = $"CALL SYSTEM$WAIT({expectedWaitTime}, \'SECONDS\');";

                // Act
                queryId = await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);
                var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);

                // Assert
                Assert.IsTrue(connections[0].IsStillRunning(queryStatus));
            }

            // Execute a normal query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[1].CreateCommand())
            {
                // Arrange
                cmd.CommandText = $"select 1;";

                // Act
                var row = cmd.ExecuteScalar();

                // Assert
                Assert.AreEqual(1, row);
            }

            // Get results of the async exec query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[2].CreateCommand())
            {
                // Act
                var reader = await cmd.GetResultsFromQueryIdAsync(queryId, CancellationToken.None).ConfigureAwait(false);
                var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);

                // Assert
                Assert.IsTrue(reader.Read());
                Assert.AreEqual($"waited {expectedWaitTime} seconds", reader.GetString(0));
                Assert.AreEqual(QueryStatus.Success, queryStatus);
            }

            for (int i = 0; i < connections.Length; i++)
            {
                await connections[i].CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task TestAsyncExecCancelWhileGettingResultsAsync()
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    CancellationTokenSource cancelToken = new CancellationTokenSource();
                    cmd.CommandText = $"CALL SYSTEM$WAIT(60, \'SECONDS\');";

                    // Act
                    var queryId = await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);
                    var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);

                    // Assert
                    Assert.IsTrue(conn.IsStillRunning(queryStatus));

                    // Act
                    cancelToken.Cancel();
                    var thrown = Assert.ThrowsAsync<OperationCanceledException>(async () =>
                        await cmd.GetResultsFromQueryIdAsync(queryId, cancelToken.Token).ConfigureAwait(false));

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains("The operation was canceled"));
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task TestFailedAsyncExecQueryThrowsErrorAsync()
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    var statusMaxRetryCount = 5;
                    var statusRetryCount = 0;
                    cmd.CommandText = $"SELECT * FROM FAKE_TABLE;";

                    // Act
                    var queryId = await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);
                    var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);
                    while (statusRetryCount < statusMaxRetryCount && conn.IsStillRunning(queryStatus))
                    {
                        Thread.Sleep(1000);
                        queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);
                        statusRetryCount++;
                    }

                    // Assert
                    Assert.AreEqual(QueryStatus.FailedWithError, queryStatus);

                    // Act
                    var thrown = Assert.ThrowsAsync<SnowflakeDbException>(async () =>
                        await cmd.GetResultsFromQueryIdAsync(queryId, CancellationToken.None).ConfigureAwait(false));

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains("'FAKE_TABLE' does not exist"));
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task TestGetStatusOfInvalidQueryIdAsync()
        {
            string fakeQueryId = "fakeQueryId";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.ThrowsAsync<Exception>(async () =>
                        await cmd.GetQueryStatusAsync(fakeQueryId, CancellationToken.None).ConfigureAwait(false));

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains($"The given query id {fakeQueryId} is not valid uuid"));
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task TestGetResultsOfInvalidQueryIdAsync()
        {
            string fakeQueryId = "fakeQueryId";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.ThrowsAsync<Exception>(async () =>
                        await cmd.GetResultsFromQueryIdAsync(fakeQueryId, CancellationToken.None).ConfigureAwait(false));

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains($"The given query id {fakeQueryId} is not valid uuid"));
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [Test, NonParallelizable]
        public async Task TestGetStatusOfUnknownQueryIdAsync()
        {
            string unknownQueryId = "ba321edc-1abc-123e-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var queryStatus = await cmd.GetQueryStatusAsync(unknownQueryId, CancellationToken.None).ConfigureAwait(false);

                    // Assert
                    Assert.AreEqual(QueryStatus.NoData, queryStatus);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [Test]
        [Ignore("The test takes too long to finish when using the default retry")]
        public async Task TestGetResultsOfUnknownQueryIdAsyncWithDefaultRetry()
        {
            string unknownQueryId = "ab123fed-1abc-987f-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.ThrowsAsync<Exception>(async () =>
                        await cmd.GetResultsFromQueryIdAsync(unknownQueryId, CancellationToken.None).ConfigureAwait(false));

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains($"Max retry for no data is reached"));
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task TestGetResultsOfUnknownQueryIdAsyncWithConfiguredRetry()
        {
            var queryResultsRetryCount = 3;
            var queryResultsRetryPattern = new int[] { 1, 2 };
            var unknownQueryId = "ab123fed-1abc-987f-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    QueryResultsAwaiter queryResultsAwaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(queryResultsRetryCount, queryResultsRetryPattern));

                    // Act
                    var thrown = Assert.ThrowsAsync<Exception>(async () =>
                        await queryResultsAwaiter.RetryUntilQueryResultIsAvailable(conn, unknownQueryId, CancellationToken.None, true).ConfigureAwait(false));

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains($"Max retry for no data is reached"));
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

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
        [Ignore("This test case takes too much time so run it manually")]
        public void TestRowsAffectedOverflowInt()
        {
            using (IDbConnection conn = new SnowflakeDbConnection(ConnectionString + "poolingEnabled=false"))
            {
                conn.Open();

                CreateOrReplaceTable(conn, TableName, new[] { "c1 NUMBER" });

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"INSERT INTO {TableName} SELECT SEQ4() FROM TABLE(GENERATOR(ROWCOUNT=>2147484000))";
                    int affected = command.ExecuteNonQuery();

                    Assert.AreEqual(-1, affected);
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
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

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
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                Assert.AreEqual(UpdateRowSource.None, cmd.UpdatedRowSource);
                try
                {
                    cmd.UpdatedRowSource = UpdateRowSource.FirstReturnedRecord;
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                Assert.AreSame(conn, cmd.Connection);
                try
                {
                    cmd.Connection = null;
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                Assert.IsFalse(((SnowflakeDbCommand)cmd).DesignTimeVisible);
                try
                {
                    ((SnowflakeDbCommand)cmd).DesignTimeVisible = true;
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(270009, e.ErrorCode);
                }

                object val = cmd.ExecuteScalar();
                Assert.AreEqual(1L, (long)val);

                conn.Close();
            }
        }

        [Test]
        public void TestSimpleLargeResultSet()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 200000)) v order by 1";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    int counter = 0;
                    while (reader.Read())
                    {
                        Assert.AreEqual(counter.ToString(), reader.GetString(0));
                        // don't test the second column as it has random values just to increase the response size
                        counter++;
                    }
                    Assert.AreEqual(200000, counter);
                }
                conn.Close();
            }
        }

        [Test, NonParallelizable]
        public void TestUseV3ResultParser()
        {
            var connectionString = ConnectionString + "poolingEnabled=false";

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 10000)) v order by 1";
                IDataReader reader = cmd.ExecuteReader();
                int counter = 0;
                while (reader.Read())
                {
                    Assert.AreEqual(counter.ToString(), reader.GetString(0));
                    // don't test the second column as it has random values just to increase the response size
                    counter++;
                }
                Assert.AreEqual(10000, counter);
            }
        }

        [Test, NonParallelizable]
        public void TestUseV3ChunkDownloader()
        {
            var connectionString = ConnectionString + "poolingEnabled=false";

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 10000)) v order by 1";
                IDataReader reader = cmd.ExecuteReader();
                int counter = 0;
                while (reader.Read())
                {
                    Assert.AreEqual(counter.ToString(), reader.GetString(0));
                    // don't test the second column as it has random values just to increase the response size
                    counter++;
                }
                Assert.AreEqual(10000, counter);
            }
        }

        [Test, NonParallelizable]
        public void TestDefaultChunkDownloaderWithPrefetchThreads([Values(1, 2, 4)] int prefetchThreads)
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection(ConnectionString + "poolingEnabled=false"))
            {
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"alter session set CLIENT_PREFETCH_THREADS = {prefetchThreads}";
                cmd.ExecuteNonQuery();

                // 10000 - value to ensure chunking occurs
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 10000)) v order by 1";

                IDataReader reader = cmd.ExecuteReader();
                int counter = 0;
                while (reader.Read())
                {
                    Assert.AreEqual(counter.ToString(), reader.GetString(0));
                    // don't test the second column as it has random values just to increase the response size
                    counter++;
                }
                Assert.AreEqual(10000, counter);
                conn.Close();
            }
        }

        [Test]
        public void TestDataSourceError()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

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
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

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
                    catch (SnowflakeDbException e)
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
        [Ignore("This test case takes too much time so run it manually")]
        public void TestQueryTimeout()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                // timelimit = 17min
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 1020)) v";
                // timeout = 16min - Using a timeout > default Rest timeout of 15min
                cmd.CommandTimeout = 16 * 60;

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
                catch (SnowflakeDbException e)
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
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

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
                command.CommandText = $"create or replace table {TableName}(cola string)";
                command.ExecuteNonQuery();
                command.Transaction.Commit();
                AddTableToRemoveList(TableName);

                command.CommandText = $"show tables like '{TableName}'";
                IDataReader reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());
                Assert.IsFalse(reader.Read());

                // start another transaction to test rollback
                tran = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                command.Transaction = tran;
                command.CommandText = $"insert into {TableName} values('test')";

                command.ExecuteNonQuery();
                command.CommandText = $"select * from {TableName}";
                reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("test", reader.GetString(0));
                command.Transaction.Rollback();

                // no value will be in table since it has been rollbacked
                command.CommandText = $"select * from {TableName}";
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
                $"create or replace table {TableName}(cola int, colb string)",
                $"insert into {TableName} values(1, 'a'),(2, 'b')",
                $"merge into {TableName} using (select 1 as cola, 'c' as colb) m on " +
                $"{TableName}.cola = m.cola when matched then update set {TableName}.colb='update' " +
                "when not matched then insert (cola, colb) values (3, 'd')",
                $"drop table if exists {TableName}"
            };

            int[] expectedResult =
            {
                0, 2, 1, 0
            };

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";

                conn.Open();

                using (IDbCommand command = conn.CreateCommand())
                {
                    int rowsAffected = -1;
                    for (int i = 0; i < testCommands.Length; i++)
                    {
                        command.CommandText = testCommands[i];
                        rowsAffected = command.ExecuteNonQuery();

                        Assert.AreEqual(expectedResult[i], rowsAffected);
                    }
                }
            }
        }

        [Test]
        public void TestExecuteScalarNull()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
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
        public void TestExecuteWithMaxRetryReached()
        {
            var mockRestRequester = new MockRetryUntilRestTimeoutRestRequester()
            {
                _forceTimeoutForNonLoginRequestsOnly = true
            };

            using (IDbConnection conn = new MockSnowflakeDbConnection(mockRestRequester))
            {
                string maxRetryConnStr = ConnectionString + "maxHttpRetries=8;poolingEnabled=false";

                conn.ConnectionString = maxRetryConnStr;
                conn.Open();

                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    using (IDbCommand command = conn.CreateCommand())
                    {
                        command.CommandText = "select 1;";
                        command.ExecuteScalar();
                    }
                    Assert.Fail();
                }
                catch (Exception e)
                {
                    Assert.IsInstanceOf<TaskCanceledException>(e.InnerException);
                }
                stopwatch.Stop();

                var totalDelaySeconds = 1 + 2 + 4 + 8 + 16 + 16 + 16 + 16;
                const int MillisecondsDifferenceToAccept = 5;
                // retry 8 times with backoff 1, 2, 4, 8, 16, 16, 16, 16 seconds
                // but should not delay more than another 16 seconds
                Assert.Less(stopwatch.ElapsedMilliseconds, (totalDelaySeconds + 20) * 1000);
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds + MillisecondsDifferenceToAccept, totalDelaySeconds * 1000);
            }
        }

        [Test]
        public void TestCreateCommandBeforeOpeningConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                using (var command = conn.CreateCommand())
                {
                    conn.Open();
                    command.CommandText = "select 1";
                    Assert.DoesNotThrow(() => command.ExecuteNonQuery());
                }
            }
        }

        [Test]
        public void TestRowsAffectedUnload()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (IDbCommand command = conn.CreateCommand())
                {
                    CreateOrReplaceTable(conn, TableName, new[] { "c1 NUMBER" });

                    command.CommandText = $"insert into {TableName} values(1), (2), (3), (4), (5), (6)";
                    command.ExecuteNonQuery();

                    command.CommandText = "drop stage if exists my_unload_stage";
                    command.ExecuteNonQuery();

                    command.CommandText = "create stage if not exists my_unload_stage";
                    command.ExecuteNonQuery();

                    command.CommandText = $"copy into @my_unload_stage/unload/ from {TableName};";
                    int affected = command.ExecuteNonQuery();

                    Assert.AreEqual(6, affected);

                    command.CommandText = "drop stage if exists my_unload_stage";
                    command.ExecuteNonQuery();
                }
                conn.Close();
            }
        }

        [Test]
        // [Ignore("Ignore flaky unstable test case for now.")]
        [Retry(2)]
        public void testPutArrayBindAsync()
        {
            ArrayBindTest(ConnectionString + "poolingEnabled=false", TableName, 7500);
        }

        private void ArrayBindTest(string connstr, string tableName, int size)
        {
            const int timeoutSeconds = 150;
            CancellationTokenSource externalCancel = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connstr;
                conn.Open();

                CreateOrReplaceTable(conn, tableName, new[]
                {
                    "cola INTEGER",
                    "colb STRING",
                    "colc DATE",
                    "cold TIME",
                    "cole TIMESTAMP_NTZ",
                    "colf TIMESTAMP_TZ"
                });

                using (DbCommand cmd = conn.CreateCommand())
                {
                    string insertCommand = "insert into " + tableName + " values (?, ?, ?, ?, ?, ?)";
                    cmd.CommandText = insertCommand;

                    int total = size;

                    List<int> arrint = new List<int>();
                    for (int i = 0; i < total; i++)
                    {
                        arrint.Add(i * 10 + 1);
                        arrint.Add(i * 10 + 2);
                        arrint.Add(i * 10 + 3);
                    }
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = arrint.ToArray();
                    cmd.Parameters.Add(p1);

                    List<string> arrstring = new List<string>();
                    for (int i = 0; i < total; i++)
                    {
                        arrstring.Add("str1");
                        arrstring.Add("str2");
                        arrstring.Add("str3\"test\"");
                    }
                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = arrstring.ToArray();
                    cmd.Parameters.Add(p2);

                    DateTime date1 = DateTime.ParseExact("2000-01-01 00:00:00.0000000", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    DateTime date2 = DateTime.ParseExact("2020-05-11 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    DateTime date3 = DateTime.ParseExact("2021-07-22 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    List<DateTime> arrDate = new List<DateTime>();
                    for (int i = 0; i < total; i++)
                    {
                        arrDate.Add(date1);
                        arrDate.Add(date2);
                        arrDate.Add(date3);
                    }
                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Date;
                    p3.Value = arrDate.ToArray();
                    cmd.Parameters.Add(p3);

                    DateTime time1 = DateTime.ParseExact("00:00:00.0000000", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    DateTime time2 = DateTime.ParseExact("23:59:59.9999999", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    DateTime time3 = DateTime.ParseExact("12:35:41.3333333", "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                    List<DateTime> arrTime = new List<DateTime>();
                    for (int i = 0; i < total; i++)
                    {
                        arrTime.Add(time1);
                        arrTime.Add(time2);
                        arrTime.Add(time3);
                    }

                    var p4 = cmd.CreateParameter();
                    p4.ParameterName = "4";
                    p4.DbType = DbType.Time;
                    p4.Value = arrTime.ToArray();
                    cmd.Parameters.Add(p4);

                    DateTime ntz1 = DateTime.ParseExact("2017-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    DateTime ntz2 = DateTime.ParseExact("2020-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    DateTime ntz3 = DateTime.ParseExact("2022-04-01 00:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    List<DateTime> arrNtz = new List<DateTime>();
                    for (int i = 0; i < total; i++)
                    {
                        arrNtz.Add(ntz1);
                        arrNtz.Add(ntz2);
                        arrNtz.Add(ntz3);
                    }
                    var p5 = cmd.CreateParameter();
                    p5.ParameterName = "5";
                    p5.DbType = DbType.DateTime2;
                    p5.Value = arrNtz.ToArray();
                    cmd.Parameters.Add(p5);

                    DateTimeOffset tz1 = DateTimeOffset.Now;
                    DateTimeOffset tz2 = DateTimeOffset.UtcNow;
                    DateTimeOffset tz3 = new DateTimeOffset(2007, 1, 1, 12, 0, 0, new TimeSpan(4, 0, 0));
                    List<DateTimeOffset> arrTz = new List<DateTimeOffset>();
                    for (int i = 0; i < total; i++)
                    {
                        arrTz.Add(tz1);
                        arrTz.Add(tz2);
                        arrTz.Add(tz3);
                    }

                    var p6 = cmd.CreateParameter();
                    p6.ParameterName = "6";
                    p6.DbType = DbType.DateTimeOffset;
                    p6.Value = arrTz.ToArray();
                    cmd.Parameters.Add(p6);

                    Task<int> task = cmd.ExecuteNonQueryAsync(externalCancel.Token);

                    if (!task.Wait(TimeSpan.FromSeconds(timeoutSeconds)))
                    {
                        Assert.Fail($"Array bind operation timed out after {timeoutSeconds} seconds");
                    }
                    Assert.AreEqual(total * 3, task.Result);

                    cmd.CommandText = "SELECT * FROM " + tableName;
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                }
                conn.Close();
            }
        }

        [Test]
        public void TestPutArrayBindAsyncMultiThreading()
        {
            var t1TableName = TableName + 1;
            var t2TableName = TableName + 2;

            Thread t1 = new Thread(() => ThreadProcess1(ConnectionString + "poolingEnabled=false", t1TableName));
            Thread t2 = new Thread(() => ThreadProcess2(ConnectionString + "poolingEnabled=false", t2TableName));
            //Thread t3 = new Thread(() => ThreadProcess3(ConnectionString));
            //Thread t4 = new Thread(() => ThreadProcess4(ConnectionString));

            t1.Start();
            t2.Start();
            //t3.Start();
            //t4.Start();
            t1.Join();
            t2.Join();
        }

        private void ThreadProcess1(string connstr, string tableName)
        {
            ArrayBindTest(connstr, tableName, 15000);
        }

        private void ThreadProcess2(string connstr, string tableName)
        {
            ArrayBindTest(connstr, tableName, 15000);
        }

        private void ThreadProcess3(string connstr, string tableName)
        {
            ArrayBindTest(connstr, tableName, 20000);
        }

        private void ThreadProcess4(string connstr, string tableName)
        {
            ArrayBindTest(connstr, tableName, 25000);
        }

        [Test]
        public void testExecuteScalarAsyncSelect()
        {
            CancellationTokenSource externalCancel = new CancellationTokenSource();
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                CreateOrReplaceTable(conn, TableName, new[] { "cola INTEGER" });

                using (DbCommand cmd = conn.CreateCommand())
                {
                    string insertCommand = $"insert into {TableName} values (?)";
                    cmd.CommandText = insertCommand;
                    int total = 1000;

                    List<int> arrint = new List<int>();
                    for (int i = 0; i < total; i++)
                    {
                        arrint.Add(i);
                    }
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = arrint.ToArray();
                    cmd.Parameters.Add(p1);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"SELECT COUNT(*) FROM {TableName}";
                    Task<object> task = cmd.ExecuteScalarAsync(externalCancel.Token);

                    task.Wait();
                    Assert.AreEqual(total, task.Result);
                }
                conn.Close();
            }
        }

        [Test]
        [IgnoreOnEnvIs("snowflake_cloud_env",
                       new string[] { "AWS", "AZURE" })]
        public void testExecuteLargeQueryWithGcsDownscopedToken()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "GCS_USE_DOWNSCOPED_CREDENTIAL=true;poolingEnabled=false";
                conn.Open();

                int rowCount = 100000;

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"SELECT COUNT(*) FROM (select seq4() from table(generator(rowcount => {rowCount})))";
                    Assert.AreEqual(rowCount, command.ExecuteScalar());
                }
                conn.Close();
            }
        }

        [Test]
        public void TestGetQueryId()
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                // query id is null when no query executed
                SnowflakeDbCommand command = (SnowflakeDbCommand)conn.CreateCommand();
                string queryId = command.GetQueryId();
                Assert.IsNull(queryId);

                // query id from ExecuteNonQuery
                command.CommandText = "create or replace temporary table testgetqueryid(cola string)";
                command.ExecuteNonQuery();
                queryId = command.GetQueryId();
                Assert.IsNotEmpty(queryId);

                // query id from ExecuteReader
                command.CommandText = "show tables like 'testgetqueryid'";
                SnowflakeDbDataReader reader = (SnowflakeDbDataReader)command.ExecuteReader();
                queryId = command.GetQueryId();
                Assert.IsNotEmpty(queryId);
                Assert.AreEqual(queryId, reader.GetQueryId());
                Assert.IsTrue(reader.Read());

                // query id from insert query
                command.CommandText = "insert into testgetqueryid values('test')";
                command.ExecuteNonQuery();
                queryId = command.GetQueryId();
                Assert.IsNotEmpty(queryId);

                // query id from select query
                command.CommandText = "select * from testgetqueryid";
                reader = (SnowflakeDbDataReader)command.ExecuteReader();
                queryId = command.GetQueryId();
                Assert.IsNotEmpty(queryId);
                Assert.AreEqual(queryId, reader.GetQueryId());
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("test", reader.GetString(0));

                // query id from different DbCommand instance
                SnowflakeDbCommand command2 = (SnowflakeDbCommand)conn.CreateCommand();
                string queryId2 = command2.GetQueryId();
                Assert.IsNull(queryId2);
                command2.CommandText = "select 'test2'";
                SnowflakeDbDataReader reader2 = (SnowflakeDbDataReader)command2.ExecuteReader();
                queryId2 = command2.GetQueryId();
                Assert.IsNotEmpty(queryId2);
                Assert.AreEqual(queryId2, reader2.GetQueryId());
                // each DbCommand instance has it's own query Id.
                Assert.AreNotEqual(queryId2, queryId);
                Assert.IsTrue(reader2.Read());
                Assert.AreEqual("test2", reader2.GetString(0));

                // use query Id to get the result
                command.CommandText = $"select * from table(result_scan('{queryId}'))";
                reader = (SnowflakeDbDataReader)command.ExecuteReader();
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("test", reader.GetString(0));

                command2.CommandText = $"select * from table(result_scan('{queryId2}'))";
                reader2 = (SnowflakeDbDataReader)command2.ExecuteReader();
                Assert.IsTrue(reader2.Read());
                Assert.AreEqual("test2", reader2.GetString(0));

                // query id from failed query
                command.CommandText = "select * from table_not_exists";
                try
                {
                    reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(2003, e.ErrorCode);
                }

                queryId = command.GetQueryId();
                Assert.IsNotEmpty(queryId);

                conn.Close();
            }
        }

        [Test]
        public void TestAsyncExecQuery()
        {
            string queryId;
            var expectedWaitTime = 5;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    cmd.CommandText = $"CALL SYSTEM$WAIT({expectedWaitTime}, \'SECONDS\');";

                    // Act
                    queryId = cmd.ExecuteInAsyncMode();
                    var queryStatus = cmd.GetQueryStatus(queryId);

                    // Assert
                    Assert.IsTrue(conn.IsStillRunning(queryStatus));
                    Assert.IsFalse(conn.IsAnError(queryStatus));

                    // Act
                    DbDataReader reader = cmd.GetResultsFromQueryId(queryId);

                    // Assert
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual($"waited {expectedWaitTime} seconds", reader.GetString(0));
                    Assert.AreEqual(QueryStatus.Success, cmd.GetQueryStatus(queryId));
                }

                conn.Close();
            }
        }

        [Test, NonParallelizable]
        public void TestExecuteNormalQueryWhileAsyncExecQueryIsRunning()
        {
            string queryId;
            var expectedWaitTime = 5;

            SnowflakeDbConnection[] connections = new SnowflakeDbConnection[3];
            for (int i = 0; i < connections.Length; i++)
            {
                connections[i] = new SnowflakeDbConnection(ConnectionString + "poolingEnabled=false");
                connections[i].Open();
            }

            // Start the async exec query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[0].CreateCommand())
            {
                // Arrange
                cmd.CommandText = $"CALL SYSTEM$WAIT({expectedWaitTime}, \'SECONDS\');";

                // Act
                queryId = cmd.ExecuteInAsyncMode();

                // Assert
                Assert.IsTrue(connections[0].IsStillRunning(cmd.GetQueryStatus(queryId)));
            }

            // Execute a normal query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[1].CreateCommand())
            {
                // Arrange
                cmd.CommandText = $"select 1;";

                // Act
                var row = cmd.ExecuteScalar();

                // Assert
                Assert.AreEqual(1, row);
            }

            // Get results of the async exec query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[2].CreateCommand())
            {
                // Act
                DbDataReader reader = cmd.GetResultsFromQueryId(queryId);

                // Assert
                Assert.IsTrue(reader.Read());
                Assert.AreEqual($"waited {expectedWaitTime} seconds", reader.GetString(0));
                Assert.AreEqual(QueryStatus.Success, cmd.GetQueryStatus(queryId));
            }

            for (int i = 0; i < connections.Length; i++)
            {
                connections[i].Close();
            }
        }

        [Test]
        public void TestFailedAsyncExecQueryThrowsError()
        {
            string queryId;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    var statusMaxRetryCount = 5;
                    var statusRetryCount = 0;
                    cmd.CommandText = $"SELECT * FROM FAKE_TABLE;";

                    // Act
                    queryId = cmd.ExecuteInAsyncMode();
                    while (statusRetryCount < statusMaxRetryCount && conn.IsStillRunning(cmd.GetQueryStatus(queryId)))
                    {
                        Thread.Sleep(1000);
                        statusRetryCount++;
                    }

                    // Assert
                    Assert.AreEqual(QueryStatus.FailedWithError, cmd.GetQueryStatus(queryId));

                    // Act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => cmd.GetResultsFromQueryId(queryId));

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains("'FAKE_TABLE' does not exist"));
                }

                conn.Close();
            }
        }

        [Test]
        public void TestAsyncExecQueryPutGetThrowsNotImplemented()
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    cmd.CommandText = $"PUT file://non_existent_file.csv @~;";

                    // Act
                    var thrown = Assert.Throws<NotImplementedException>(() => cmd.ExecuteInAsyncMode());

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains("Get and Put are not supported in async execution mode"));

                    // Arrange
                    cmd.CommandText = "GET @~ file://C:\\tmp\\;";

                    // Act
                    thrown = Assert.Throws<NotImplementedException>(() => cmd.ExecuteInAsyncMode());

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains("Get and Put are not supported in async execution mode"));
                }

                conn.Close();
            }
        }

        [Test]
        public void TestGetStatusOfInvalidQueryId()
        {
            string fakeQueryId = "fakeQueryId";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.Throws<Exception>(() => cmd.GetQueryStatus(fakeQueryId));

                    // Assert
                    Assert.IsTrue(thrown.Message.Contains($"The given query id {fakeQueryId} is not valid uuid"));
                }

                conn.Close();
            }
        }

        [Test]
        public void TestGetResultsOfInvalidQueryId()
        {
            string fakeQueryId = "fakeQueryId";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.Throws<AggregateException>(() => cmd.GetResultsFromQueryId(fakeQueryId));

                    // Assert
                    Assert.IsTrue(thrown.InnerException.Message.Contains($"The given query id {fakeQueryId} is not valid uuid"));
                }

                conn.Close();
            }
        }

        [Test, NonParallelizable]
        public void TestGetStatusOfUnknownQueryId()
        {
            string unknownQueryId = "ab123cde-1cba-789a-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var queryStatus = cmd.GetQueryStatus(unknownQueryId);

                    // Assert
                    Assert.AreEqual(QueryStatus.NoData, queryStatus);
                }

                conn.Close();
            }
        }

        [Test]
        [Ignore("The test takes too long to finish when using the default retry")]
        public void TestGetResultsOfUnknownQueryIdWithDefaultRetry()
        {
            string unknownQueryId = "ba987def-1abc-987f-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.Throws<AggregateException>(() => cmd.GetResultsFromQueryId(unknownQueryId));

                    // Assert
                    Assert.IsTrue(thrown.InnerException.Message.Contains($"Max retry for no data is reached"));
                }

                conn.Close();
            }
        }

        [Test]
        public void TestGetResultsOfUnknownQueryIdWithConfiguredRetry()
        {
            var queryResultsRetryCount = 3;
            var queryResultsRetryPattern = new int[] { 1, 2 };
            var unknownQueryId = "ba987def-1abc-987f-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    QueryResultsAwaiter queryResultsAwaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(queryResultsRetryCount, queryResultsRetryPattern));
                    var task = queryResultsAwaiter.RetryUntilQueryResultIsAvailable(conn, unknownQueryId, CancellationToken.None, false);

                    // Act
                    var thrown = Assert.Throws<AggregateException>(() => task.Wait());

                    // Assert
                    Assert.IsTrue(thrown.InnerException.Message.Contains($"Max retry for no data is reached"));
                }

                conn.Close();
            }
        }

        [Test]
        public void TestSetQueryTagOverridesConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string expectedQueryTag = "Test QUERY_TAG 12345";
                string connectQueryTag = "Test 123";
                conn.ConnectionString = ConnectionString + $";query_tag={connectQueryTag}";

                conn.Open();
                var command = conn.CreateCommand();
                ((SnowflakeDbCommand)command).QueryTag = expectedQueryTag;
                // This query itself will be part of the history and will have the query tag
                command.CommandText = "SELECT QUERY_TAG FROM table(information_schema.query_history_by_session())";
                var queryTag = command.ExecuteScalar();

                Assert.AreEqual(expectedQueryTag, queryTag);
            }
        }

        [Test]
        public void TestCommandWithCommentEmbedded()
        {
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();

                command.CommandText = "\r\nselect '--'\r\n";
                var reader = command.ExecuteReader();

                Assert.IsTrue(reader.Read());
                Assert.AreEqual("--", reader.GetString(0));
            }
        }

        [Test]
        public async Task TestCommandWithCommentEmbeddedAsync()
        {
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand();

                command.CommandText = "\r\nselect '--'\r\n";
                var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                Assert.IsTrue(await reader.ReadAsync().ConfigureAwait(false));
                Assert.AreEqual("--", reader.GetString(0));
            }
        }

        [Test]
        public void TestExecuteNonQueryReturnsCorrectRowCountForUploadWithMultipleFiles()
        {
            const int NumberOfFiles = 5;
            const int NumberOfRows = 3;
            const int ExpectedRowCount = NumberOfFiles * NumberOfRows;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    var tempFolder = $"{Path.GetTempPath()}Temp_{Guid.NewGuid()}";

                    try
                    {
                        // Arrange
                        Directory.CreateDirectory(tempFolder);
                        var data = string.Concat(Enumerable.Repeat(string.Join(",", "TestData") + "\n", NumberOfRows));
                        for (int i = 0; i < NumberOfFiles; i++)
                        {
                            File.WriteAllText(Path.Combine(tempFolder, $"{TestContext.CurrentContext.Test.Name}_{i}.csv"), data);
                        }
                        CreateOrReplaceTable(conn, TableName, new[] { "COL1 STRING" });
                        cmd.CommandText = $"PUT file://{Path.Combine(tempFolder, "*.csv")} @%{TableName} AUTO_COMPRESS=FALSE";
                        var reader = cmd.ExecuteReader();

                        // Act
                        cmd.CommandText = $"COPY INTO {TableName} FROM @%{TableName} PATTERN='.*.csv' FILE_FORMAT=(TYPE=CSV)";
                        int actualRowCount = cmd.ExecuteNonQuery();

                        // Assert
                        Assert.AreEqual(ExpectedRowCount, actualRowCount);
                    }
                    finally
                    {
                        Directory.Delete(tempFolder, true);
                    }
                }
            }
        }

        [Test]
        public async Task TestExecuteNonQueryAsyncReturnsCorrectRowCountForUploadWithMultipleFiles()
        {
            const int NumberOfFiles = 5;
            const int NumberOfRows = 3;
            const int ExpectedRowCount = NumberOfFiles * NumberOfRows;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false";
                conn.Open();

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    var tempFolder = $"{Path.GetTempPath()}Temp_{Guid.NewGuid()}";

                    try
                    {
                        // Arrange
                        Directory.CreateDirectory(tempFolder);
                        var data = string.Concat(Enumerable.Repeat(string.Join(",", "TestData") + "\n", NumberOfRows));
                        for (int i = 0; i < NumberOfFiles; i++)
                        {
                            File.WriteAllText(Path.Combine(tempFolder, $"{TestContext.CurrentContext.Test.Name}_{i}.csv"), data);
                        }
                        CreateOrReplaceTable(conn, TableName, new[] { "COL1 STRING" });
                        cmd.CommandText = $"PUT file://{Path.Combine(tempFolder, "*.csv")} @%{TableName} AUTO_COMPRESS=FALSE";
                        var reader = cmd.ExecuteReader();

                        // Act
                        cmd.CommandText = $"COPY INTO {TableName} FROM @%{TableName} PATTERN='.*.csv' FILE_FORMAT=(TYPE=CSV)";
                        int actualRowCount = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

                        // Assert
                        Assert.AreEqual(ExpectedRowCount, actualRowCount);
                    }
                    finally
                    {
                        Directory.Delete(tempFolder, true);
                    }
                }
            }
        }
    }
}
