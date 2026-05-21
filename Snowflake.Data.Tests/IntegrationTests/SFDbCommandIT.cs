using System.Data;
using System.Data.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using System.Linq;
using System.IO;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Telemetry;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    class SFDbCommandIT : SFBaseTest
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
                            Assert.True(reader.Read());
                            queryResult = reader.GetInt64(0);
                            Assert.False(reader.Read());
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
                                    Assert.True(reader.Read());
                                    queryResult = reader.GetInt64(0);
                                    Assert.False(reader.Read());
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
                    Assert.True(thrown.Message.Contains($"The given query id {fakeQueryId} is not valid uuid"));
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
                    Assert.True(thrown.Message.Contains($"The given query id {fakeQueryId} is not valid uuid"));
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
                    Assert.True(thrown.Message.Contains($"Max retry for no data is reached"));
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
                    QueryResultsAwaiter queryResultsAwaiter =
                        new QueryResultsAwaiter(new QueryResultsRetryConfig(queryResultsRetryCount, queryResultsRetryPattern));

                    // Act
                    var thrown = Assert.ThrowsAsync<Exception>(async () =>
                        await queryResultsAwaiter.RetryUntilQueryResultIsAvailable(conn, unknownQueryId, CancellationToken.None, true)
                            .ConfigureAwait(false));

                    // Assert
                    Assert.True(thrown.Message.Contains($"Max retry for no data is reached"));
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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
        [TimeSensitive]
        public async Task TestCancelQuery()
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

                await Task.Delay(8000);
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
    }
}
