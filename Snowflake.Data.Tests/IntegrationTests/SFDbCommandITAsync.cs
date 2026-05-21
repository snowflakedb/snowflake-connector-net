using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests;

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
    public async Task TestCancelExecuteAsync()
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
            await Task.Delay(2000).ConfigureAwait(false);
            conn.Close();
        }
    }

    [Test]
    public void TestExecuteAsyncWithMaxRetryReached()
    {
        var mockRestRequester = new MockRetryUntilRestTimeoutRestRequester(false);

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
    [TimeSensitive("If this takes too long, query will be in success state.")]
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
                Assert.IsTrue(conn.IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");
                Assert.IsFalse(conn.IsAnError(queryStatus), $"Expected query to not be an error but status was: {queryStatus}");

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
            Assert.IsTrue(connections[0].IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");
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
                Assert.IsTrue(conn.IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");

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
    public async Task TestAsyncExecCancelAbortsQueryOnServer()
    {
        string queryId;

        using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = ConnectionString + "poolingEnabled=false";
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
            {
                // Arrange: submit a 60-second query via async mode
                CancellationTokenSource cancelToken = new CancellationTokenSource();
                cmd.CommandText = $"CALL SYSTEM$WAIT(60, \'SECONDS\');";

                queryId = await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);
                var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);
                Assert.IsTrue(conn.IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");

                // Act: cancel while polling for results
                cancelToken.CancelAfter(TimeSpan.FromSeconds(3));
                try
                {
                    await cmd.GetResultsFromQueryIdAsync(queryId, cancelToken.Token).ConfigureAwait(false);
                    Assert.Fail("Expected OperationCanceledException");
                }
                catch (OperationCanceledException)
                {
                    // TaskCanceledException is a subclass of OperationCanceledException
                }
            }

            // Assert: poll query status via REST API until the cancel completes
            using (SnowflakeDbCommand checkCmd = (SnowflakeDbCommand)conn.CreateCommand())
            {
                QueryStatus queryStatus;
                var maxRetries = 30;
                var retryCount = 0;
                do
                {
                    await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                    queryStatus = await checkCmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);
                    retryCount++;
                } while (retryCount < maxRetries && (conn.IsStillRunning(queryStatus) || queryStatus == QueryStatus.Aborting));

                Assert.That(queryStatus, Is.EqualTo(QueryStatus.FailedWithError).Or.EqualTo(QueryStatus.Aborted),
                    "Cancelled query should reach a terminal error state on the server");
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
                    await Task.Delay(1000);
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
