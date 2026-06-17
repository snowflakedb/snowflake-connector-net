using System.Data;
using System.Data.Common;
using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using System.Linq;
using System.IO;
using Snowflake.Data.Telemetry;

namespace Snowflake.Data.Tests.IntegrationTests
{
    using Xunit;
    using Snowflake.Data.Client;
    using Snowflake.Data.Configuration;
    using Snowflake.Data.Tests.Util;
    using System.Diagnostics;
    using System.Collections.Generic;
    using System.Globalization;
    using Snowflake.Data.Tests.Mock;

    public class SFDbCommandITAsync : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFDbCommandITAsync(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        [SFFact]
        public async Task TestCancelExecuteAsync()
        {
            CancellationTokenSource externalCancel = new CancellationTokenSource(TimeSpan.FromSeconds(8));

            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None);

                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 20)) v";
                // external cancellation should be triggered before timeout
                cmd.CommandTimeout = 10;
                try
                {
                    await cmd.ExecuteScalarAsync(externalCancel.Token);
                    Assert.Fail();
                }
                catch
                {
                    // assert that cancel is not triggered by timeout, but external cancellation
                    Assert.True(externalCancel.IsCancellationRequested);
                }
                await Task.Delay(2000);
                await conn.CloseAsync();
            }
        }

        [SFFact]
        public async Task TestAsyncExecQueryAsync()
        {
            string queryId;
            var expectedWaitTime = 5;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    cmd.CommandText = $"CALL SYSTEM$WAIT({expectedWaitTime}, \'SECONDS\');";

                    // Act
                    queryId = await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);
                    var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);

                    // Assert
                    Assert.True(conn.IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");
                    Assert.False(conn.IsAnError(queryStatus), $"Expected query to not be an error but status was: {queryStatus}");

                    // Act
                    DbDataReader reader = cmd.GetResultsFromQueryId(queryId);
                    queryStatus = cmd.GetQueryStatus(queryId);

                    // Assert
                    Assert.True(reader.Read());
                    Assert.Equal($"waited {expectedWaitTime} seconds", reader.GetString(0));
                    Assert.Equal(QueryStatus.Success, queryStatus);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestExecuteNormalQueryWhileAsyncExecQueryIsRunningAsync()
        {
            string queryId;
            var expectedWaitTime = 5;

            SnowflakeDbConnection[] connections = new SnowflakeDbConnection[3];
            for (int i = 0; i < connections.Length; i++)
            {
                connections[i] = new SnowflakeDbConnection(_fixture.ConnectionString + "poolingEnabled=false");
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
                Assert.True(connections[0].IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");
            }

            // Execute a normal query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[1].CreateCommand())
            {
                // Arrange
                cmd.CommandText = $"select 1;";

                // Act
                var row = cmd.ExecuteScalar();

                // Assert
                Assert.Equal((long)1, row);
            }

            // Get results of the async exec query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[2].CreateCommand())
            {
                // Act
                var reader = await cmd.GetResultsFromQueryIdAsync(queryId, CancellationToken.None).ConfigureAwait(false);
                var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);

                // Assert
                Assert.True(reader.Read());
                Assert.Equal($"waited {expectedWaitTime} seconds", reader.GetString(0));
                Assert.Equal(QueryStatus.Success, queryStatus);
            }

            for (int i = 0; i < connections.Length; i++)
            {
                await connections[i].CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestAsyncExecCancelWhileGettingResultsAsync()
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
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
                    Assert.True(conn.IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");

                    // Act
                    cancelToken.Cancel();
                    var thrown = await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                        await cmd.GetResultsFromQueryIdAsync(queryId, cancelToken.Token).ConfigureAwait(false));

                    // Assert
                    Assert.Contains("The operation was canceled", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestAsyncExecCancelAbortsQueryOnServer()
        {
            string queryId;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange: submit a 60-second query via async mode
                    CancellationTokenSource cancelToken = new CancellationTokenSource();
                    cmd.CommandText = $"CALL SYSTEM$WAIT(60, \'SECONDS\');";

                    queryId = await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);
                    var queryStatus = await cmd.GetQueryStatusAsync(queryId, CancellationToken.None).ConfigureAwait(false);
                    Assert.True(conn.IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");

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

                    Assert.True(queryStatus == QueryStatus.FailedWithError || queryStatus == QueryStatus.Aborted);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestFailedAsyncExecQueryThrowsErrorAsync()
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
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
                    Assert.Equal(QueryStatus.FailedWithError, queryStatus);

                    // Act
                    var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
                        await cmd.GetResultsFromQueryIdAsync(queryId, CancellationToken.None).ConfigureAwait(false));

                    // Assert
                    Assert.Contains("'FAKE_TABLE' does not exist", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetStatusOfInvalidQueryIdAsync()
        {
            string fakeQueryId = "fakeQueryId";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = await Assert.ThrowsAsync<Exception>(async () =>
                        await cmd.GetQueryStatusAsync(fakeQueryId, CancellationToken.None).ConfigureAwait(false));

                    // Assert
                    Assert.Contains("Invalid query id format. Expected a UUID.", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetResultsOfInvalidQueryIdAsync()
        {
            string fakeQueryId = "fakeQueryId";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = await Assert.ThrowsAsync<Exception>(async () =>
                        await cmd.GetResultsFromQueryIdAsync(fakeQueryId, CancellationToken.None).ConfigureAwait(false));

                    // Assert
                    Assert.Contains("Invalid query id format. Expected a UUID.", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetStatusOfUnknownQueryIdAsync()
        {
            string unknownQueryId = "ba321edc-1abc-123e-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var queryStatus = await cmd.GetQueryStatusAsync(unknownQueryId, CancellationToken.None).ConfigureAwait(false);

                    // Assert
                    Assert.Equal(QueryStatus.NoData, queryStatus);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact(Skip = "The test takes too long to finish when using the default retry")]
        public async Task TestGetResultsOfUnknownQueryIdAsyncWithDefaultRetry()
        {
            string unknownQueryId = "ab123fed-1abc-987f-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = await Assert.ThrowsAsync<Exception>(async () =>
                        await cmd.GetResultsFromQueryIdAsync(unknownQueryId, CancellationToken.None).ConfigureAwait(false));

                    // Assert
                    Assert.Contains("Max retry for no data is reached", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetResultsOfUnknownQueryIdAsyncWithConfiguredRetry()
        {
            var queryResultsRetryCount = 3;
            var queryResultsRetryPattern = new int[] { 1, 2 };
            var unknownQueryId = "ab123fed-1abc-987f-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    QueryResultsAwaiter queryResultsAwaiter = new QueryResultsAwaiter(new QueryResultsRetryConfig(queryResultsRetryCount, queryResultsRetryPattern));

                    // Act
                    var thrown = await Assert.ThrowsAsync<Exception>(async () =>
                        await queryResultsAwaiter.RetryUntilQueryResultIsAvailable(conn, unknownQueryId, CancellationToken.None, true).ConfigureAwait(false));

                    // Assert
                    Assert.Contains("Max retry for no data is reached", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestSimpleCommand()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None);
                var cmd = conn.CreateCommand();
                cmd.CommandText = "select 1";

                // command type can only be text, stored procedure are not supported.
                Assert.Equal(CommandType.Text, cmd.CommandType);
                try
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(270009, e.ErrorCode);
                }

                Assert.Equal(UpdateRowSource.None, cmd.UpdatedRowSource);
                try
                {
                    cmd.UpdatedRowSource = UpdateRowSource.FirstReturnedRecord;
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(270009, e.ErrorCode);
                }

                Assert.Same(conn, cmd.Connection);
                try
                {
                    cmd.Connection = null;
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(270009, e.ErrorCode);
                }

                Assert.False(((SnowflakeDbCommand)cmd).DesignTimeVisible);
                try
                {
                    ((SnowflakeDbCommand)cmd).DesignTimeVisible = true;
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(270009, e.ErrorCode);
                }

                object val = cmd.ExecuteScalar();
                Assert.Equal(1L, (long)val);

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestSimpleLargeResultSet()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None);

                var cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 200000)) v order by 1";
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    int counter = 0;
                    while (await reader.ReadAsync())
                    {
                        Assert.Equal(counter.ToString(), reader.GetString(0));
                        // don't test the second column as it has random values just to increase the response size
                        counter++;
                    }
                    Assert.Equal(200000, counter);
                }
                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestUseV3ResultParser()
        {
            var connectionString = _fixture.ConnectionString + "poolingEnabled=false";

            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                await conn.OpenAsync(CancellationToken.None);

                var cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 10000)) v order by 1";
                var reader = await cmd.ExecuteReaderAsync();
                int counter = 0;
                while (await reader.ReadAsync())
                {
                    Assert.Equal(counter.ToString(), reader.GetString(0));
                    // don't test the second column as it has random values just to increase the response size
                    counter++;
                }
                Assert.Equal(10000, counter);
            }
        }

        [SFFact]
        public async Task TestUseV3ChunkDownloader()
        {
            var connectionString = _fixture.ConnectionString + "poolingEnabled=false";

            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                await conn.OpenAsync(CancellationToken.None);

                var cmd = conn.CreateCommand();
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 10000)) v order by 1";
                var reader = await cmd.ExecuteReaderAsync();
                int counter = 0;
                while (await reader.ReadAsync())
                {
                    Assert.Equal(counter.ToString(), reader.GetString(0));
                    // don't test the second column as it has random values just to increase the response size
                    counter++;
                }
                Assert.Equal(10000, counter);
            }
        }

        [SFTheory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        public async Task TestDefaultChunkDownloaderWithPrefetchThreads(int prefetchThreads)
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection(_fixture.ConnectionString + "poolingEnabled=false"))
            {
                await conn.OpenAsync(CancellationToken.None);

                var cmd = conn.CreateCommand();
                cmd.CommandText = $"alter session set CLIENT_PREFETCH_THREADS = {prefetchThreads}";
                await cmd.ExecuteNonQueryAsync();

                // 10000 - value to ensure chunking occurs
                cmd.CommandText = "select seq4(), uniform(1, 10, 42) from table(generator(rowcount => 10000)) v order by 1";

                var reader = await cmd.ExecuteReaderAsync();
                int counter = 0;
                while (await reader.ReadAsync())
                {
                    Assert.Equal(counter.ToString(), reader.GetString(0));
                    // don't test the second column as it has random values just to increase the response size
                    counter++;
                }
                Assert.Equal(10000, counter);
                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestDataSourceError()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None);

                var cmd = conn.CreateCommand();
                cmd.CommandText = "select * from table_not_exists";
                try
                {
                    var reader = await cmd.ExecuteReaderAsync();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(2003, e.ErrorCode);
                    Assert.NotEqual("", e.QueryId);
                }

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestCancelQuery()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None);

                var cmd = conn.CreateCommand();
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
                    if (e.InnerException.GetType() != typeof(Xunit.Sdk.XunitException))
                    {
                        Assert.Equal(
                        "System.Threading.Tasks.TaskCanceledException",
                        e.InnerException.GetType().ToString());
                    }
                    else
                    {
                        // Unexpected exception
                        throw;
                    }
                }

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact(Skip = "This test case takes too much time so run it manually")]
        public async Task TestQueryTimeout()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None);

                var cmd = conn.CreateCommand();
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
                    Assert.True(stopwatch.ElapsedMilliseconds < 17 * 60 * 1000);
                    // Should timeout after the defined query timeout of 16min
                    Assert.True(stopwatch.ElapsedMilliseconds >= 16 * 60 * 1000);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    // 604 is error code from server meaning query has been canceled
                    Assert.Equal(604, e.ErrorCode);
                }

                await conn.CloseAsync(CancellationToken.None);
            }

        }

        [SFFact]
        public async Task TestTransaction()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None);

                try
                {
                    await conn.BeginTransactionAsync(IsolationLevel.ReadUncommitted);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(270009, e.ErrorCode);
                }

                var tran = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted);

                var command = conn.CreateCommand();
                command.Transaction = tran;
                command.CommandText = $"create or replace table {tableName}(cola string)";
                await command.ExecuteNonQueryAsync();
                await command.Transaction.CommitAsync();
                _fixture.AddTableToRemoveList(tableName);

                command.CommandText = $"show tables like '{tableName}'";
                var reader = await command.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                Assert.False(await reader.ReadAsync());

                // start another transaction to test rollback
                tran = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted);
                command.Transaction = tran;
                command.CommandText = $"insert into {tableName} values('test')";

                await command.ExecuteNonQueryAsync();
                command.CommandText = $"select * from {tableName}";
                reader = await command.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                Assert.Equal("test", reader.GetString(0));
                await command.Transaction.RollbackAsync();

                // no value will be in table since it has been rollbacked
                command.CommandText = $"select * from {tableName}";
                reader = await command.ExecuteReaderAsync();
                Assert.False(await reader.ReadAsync());

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestRowsAffected()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            String[] testCommands =
            {
                $"create or replace table {tableName}(cola int, colb string)",
                $"insert into {tableName} values(1, 'a'),(2, 'b')",
                $"merge into {tableName} using (select 1 as cola, 'c' as colb) m on " +
                $"{tableName}.cola = m.cola when matched then update set {tableName}.colb='update' " +
                "when not matched then insert (cola, colb) values (3, 'd')",
                $"drop table if exists {tableName}"
            };

            int[] expectedResult =
            {
                0, 2, 1, 0
            };

            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None);

                using (var command = conn.CreateCommand())
                {
                    int rowsAffected = -1;
                    for (int i = 0; i < testCommands.Length; i++)
                    {
                        command.CommandText = testCommands[i];
                        rowsAffected = await command.ExecuteNonQueryAsync();

                        Assert.Equal(expectedResult[i], rowsAffected);
                    }
                }
            }
        }

        [SFFact]
        public async Task TestExecuteScalarNull()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

                using (var command = conn.CreateCommand())
                {
                    command.CommandText = "select 1 where 2 > 3";
                    object val = await command.ExecuteScalarAsync();

                    Assert.Equal(DBNull.Value, val);
                }
                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestCreateCommandBeforeOpeningConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;

                using (var command = conn.CreateCommand())
                {
                    await conn.OpenAsync(CancellationToken.None);
                    command.CommandText = "select 1";
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        [SFFact]
        public async Task TestRowsAffectedUnload()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

                using (var command = conn.CreateCommand())
                {
                    await _fixture.CreateOrReplaceTable(conn, tableName, new[] { "c1 NUMBER" });

                    command.CommandText = $"insert into {tableName} values(1), (2), (3), (4), (5), (6)";
                    await command.ExecuteNonQueryAsync();

                    command.CommandText = "drop stage if exists my_unload_stage";
                    await command.ExecuteNonQueryAsync();

                    command.CommandText = "create stage if not exists my_unload_stage";
                    await command.ExecuteNonQueryAsync();

                    command.CommandText = $"copy into @my_unload_stage/unload/ from {tableName};";
                    int affected = await command.ExecuteNonQueryAsync();

                    Assert.Equal(6, affected);

                    command.CommandText = "drop stage if exists my_unload_stage";
                    await command.ExecuteNonQueryAsync();
                }
                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task testPutArrayBindAsync()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            await ArrayBindTest(_fixture.ConnectionString + "poolingEnabled=false", tableName, 7500);
        }

        private async Task ArrayBindTest(string connstr, string tableName, int size)
        {
            const int timeoutSeconds = 150;
            CancellationTokenSource externalCancel = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connstr;
                await conn.OpenAsync(CancellationToken.None);

                await _fixture.CreateOrReplaceTable(conn, tableName, new[]
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
                    Assert.Equal(total * 3, task.Result);

                    cmd.CommandText = "SELECT * FROM " + tableName;
                    var reader = await cmd.ExecuteReaderAsync();
                    Assert.True(await reader.ReadAsync(externalCancel.Token));
                }
                await conn.CloseAsync();
            }
        }

        [SFFact]
        public void TestPutArrayBindAsyncMultiThreading()
        {
            var t1TableName = _fixture.TableNameBaseName + "1" + Guid.NewGuid().ToString("N");
            var t2TableName = _fixture.TableNameBaseName + "2" + Guid.NewGuid().ToString("N");

            Thread t1 = new Thread(() => ThreadProcess1(_fixture.ConnectionString + "poolingEnabled=false", t1TableName));
            Thread t2 = new Thread(() => ThreadProcess2(_fixture.ConnectionString + "poolingEnabled=false", t2TableName));
            //Thread t3 = new Thread(() => ThreadProcess3(_fixture.ConnectionString));
            //Thread t4 = new Thread(() => ThreadProcess4(_fixture.ConnectionString));

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

        [SFFact]
        public async Task testExecuteScalarAsyncSelect()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            CancellationTokenSource externalCancel = new CancellationTokenSource();
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

                await _fixture.CreateOrReplaceTable(conn, tableName, new[] { "cola INTEGER" });

                using (DbCommand cmd = conn.CreateCommand())
                {
                    string insertCommand = $"insert into {tableName} values (?)";
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
                    await cmd.ExecuteNonQueryAsync();

                    cmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
                    var result = await cmd.ExecuteScalarAsync(externalCancel.Token);

                    Assert.Equal((long)total, result);
                }
                await conn.CloseAsync();
            }
        }

        [SFFact(SkipCondition.SkipOnCloudAWS | SkipCondition.SkipOnCloudAzure, RetriesCount = RetriesCount.Thrice)]
        public async Task testExecuteLargeQueryWithGcsDownscopedToken()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "GCS_USE_DOWNSCOPED_CREDENTIAL=true;poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

                var rowCount = 100000L;

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = $"SELECT COUNT(*) FROM (select seq4() from table(generator(rowcount => {rowCount})))";
                    Assert.Equal(rowCount, command.ExecuteScalar());
                }
                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestGetQueryId()
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

                // query id is null when no query executed
                SnowflakeDbCommand command = (SnowflakeDbCommand)conn.CreateCommand();
                string queryId = command.GetQueryId();
                Assert.Null(queryId);

                // query id from ExecuteNonQuery
                command.CommandText = "create or replace temporary table testgetqueryid(cola string)";
                command.ExecuteNonQuery();
                queryId = command.GetQueryId();
                Assert.NotEmpty(queryId);

                // query id from ExecuteReader
                command.CommandText = "show tables like 'testgetqueryid'";
                SnowflakeDbDataReader reader = (SnowflakeDbDataReader)command.ExecuteReader();
                queryId = command.GetQueryId();
                Assert.NotEmpty(queryId);
                Assert.Equal(queryId, reader.GetQueryId());
                Assert.True(reader.Read());

                // query id from insert query
                command.CommandText = "insert into testgetqueryid values('test')";
                command.ExecuteNonQuery();
                queryId = command.GetQueryId();
                Assert.NotEmpty(queryId);

                // query id from select query
                command.CommandText = "select * from testgetqueryid";
                reader = (SnowflakeDbDataReader)command.ExecuteReader();
                queryId = command.GetQueryId();
                Assert.NotEmpty(queryId);
                Assert.Equal(queryId, reader.GetQueryId());
                Assert.True(reader.Read());
                Assert.Equal("test", reader.GetString(0));

                // query id from different DbCommand instance
                SnowflakeDbCommand command2 = (SnowflakeDbCommand)conn.CreateCommand();
                string queryId2 = command2.GetQueryId();
                Assert.Null(queryId2);
                command2.CommandText = "select 'test2'";
                SnowflakeDbDataReader reader2 = (SnowflakeDbDataReader)await command2.ExecuteReaderAsync();
                queryId2 = command2.GetQueryId();
                Assert.NotEmpty(queryId2);
                Assert.Equal(queryId2, reader2.GetQueryId());
                // each DbCommand instance has it's own query Id.
                Assert.NotEqual(queryId2, queryId);
                Assert.True(reader2.Read());
                Assert.Equal("test2", reader2.GetString(0));

                // use query Id to get the result
                command.CommandText = $"select * from table(result_scan('{queryId}'))";
                reader = (SnowflakeDbDataReader)command.ExecuteReader();
                Assert.True(reader.Read());
                Assert.Equal("test", reader.GetString(0));

                command2.CommandText = $"select * from table(result_scan('{queryId2}'))";
                reader2 = (SnowflakeDbDataReader)await command2.ExecuteReaderAsync();
                Assert.True(reader2.Read());
                Assert.Equal("test2", reader2.GetString(0));

                // query id from failed query
                command.CommandText = "select * from table_not_exists";
                try
                {
                    reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(2003, e.ErrorCode);
                }

                queryId = command.GetQueryId();
                Assert.NotEmpty(queryId);

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestAsyncExecQuery()
        {
            string queryId;
            var expectedWaitTime = 5;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    cmd.CommandText = $"CALL SYSTEM$WAIT({expectedWaitTime}, \'SECONDS\');";

                    // Act
                    queryId = cmd.ExecuteInAsyncMode();
                    var queryStatus = cmd.GetQueryStatus(queryId);

                    // Assert
                    Assert.True(conn.IsStillRunning(queryStatus), $"Expected query to still be running but status was: {queryStatus}");
                    Assert.False(conn.IsAnError(queryStatus), $"Expected query to not be an error but status was: {queryStatus}");

                    // Act
                    DbDataReader reader = cmd.GetResultsFromQueryId(queryId);

                    // Assert
                    Assert.True(reader.Read());
                    Assert.Equal($"waited {expectedWaitTime} seconds", reader.GetString(0));
                    Assert.Equal(QueryStatus.Success, cmd.GetQueryStatus(queryId));
                }

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestExecuteNormalQueryWhileAsyncExecQueryIsRunning()
        {
            string queryId;
            var expectedWaitTime = 5;

            SnowflakeDbConnection[] connections = new SnowflakeDbConnection[3];
            for (int i = 0; i < connections.Length; i++)
            {
                connections[i] = new SnowflakeDbConnection(_fixture.ConnectionString + "poolingEnabled=false");
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
                Assert.True(connections[0].IsStillRunning(cmd.GetQueryStatus(queryId)));
            }

            // Execute a normal query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[1].CreateCommand())
            {
                // Arrange
                cmd.CommandText = $"select 1;";

                // Act
                var row = cmd.ExecuteScalar();

                // Assert
                Assert.Equal(1L, row);
            }

            // Get results of the async exec query
            using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)connections[2].CreateCommand())
            {
                // Act
                DbDataReader reader = cmd.GetResultsFromQueryId(queryId);

                // Assert
                Assert.True(await reader.ReadAsync());
                Assert.Equal($"waited {expectedWaitTime} seconds", reader.GetString(0));
                Assert.Equal(QueryStatus.Success, cmd.GetQueryStatus(queryId));
            }

            for (int i = 0; i < connections.Length; i++)
            {
                connections[i].Close();
            }
        }

        [SFFact]
        public async Task TestFailedAsyncExecQueryThrowsError()
        {
            string queryId;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

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
                        await Task.Delay(1000).ConfigureAwait(false);
                        statusRetryCount++;
                    }

                    // Assert
                    Assert.Equal(QueryStatus.FailedWithError, cmd.GetQueryStatus(queryId));

                    // Act
                    var thrown = Assert.Throws<SnowflakeDbException>(() => cmd.GetResultsFromQueryId(queryId));

                    // Assert
                    Assert.Contains("'FAKE_TABLE' does not exist", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestAsyncExecQueryPutGetThrowsNotImplemented()
        {
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    cmd.CommandText = $"PUT file://non_existent_file.csv @~;";

                    // Act
                    var thrown = Assert.Throws<NotImplementedException>(() => cmd.ExecuteInAsyncMode());

                    // Assert
                    Assert.Contains("Get and Put are not supported in async execution mode", thrown.Message);

                    // Arrange
                    cmd.CommandText = "GET @~ file://C:\\tmp\\;";

                    // Act
                    thrown = Assert.Throws<NotImplementedException>(() => cmd.ExecuteInAsyncMode());

                    // Assert
                    Assert.Contains("Get and Put are not supported in async execution mode", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFTheory]
        [InlineData("ExecuteNonQuery")]
        [InlineData("ExecuteScalar")]
        [InlineData("ExecuteReader")]
        public void TestSyncCommandExecutionEmitsTelemetry(string method)
        {
            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString =
                $"{_fixture.ConnectionString.Replace($"CLIENT_TELEMETRY_ENABLED={false}", $"CLIENT_TELEMETRY_ENABLED={true}")}poolingEnabled=false";
            conn.Open();

            var capturedActivities = new List<Activity>();
            using var listener = new ActivityListener();
            listener.ShouldListenTo = source => source.Name == ActivityStarter.ActivitySourceName;
            listener.Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData;
            listener.ActivityStopped = activity =>
            {
                if (activity.GetTagItem(TelemetryTags.SessionId)?.Equals(conn.SfSession.sessionId) == true)
                    capturedActivities.Add(activity);
            };
            ActivitySource.AddActivityListener(listener);

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            // Act
            switch (method)
            {
                case "ExecuteNonQuery": cmd.ExecuteNonQuery(); break;
                case "ExecuteScalar": cmd.ExecuteScalar(); break;
                case "ExecuteReader": cmd.ExecuteReader().Dispose(); break;
            }

            // Assert
            var expectedOp = method switch
            {
                "ExecuteNonQuery" => TelemetryActivities.ExecuteNonQuery,
                "ExecuteScalar" => TelemetryActivities.ExecuteScalar,
                "ExecuteReader" => TelemetryActivities.ExecuteDbDataReader,
                _ => throw new ArgumentException(method)
            };
            SpinWait.SpinUntil(capturedActivities.Any, TimeSpan.FromSeconds(30));
            AssertSingleTelemetryActivity(capturedActivities, expectedOp);
        }

        [SFTheory]
        [InlineData("ExecuteNonQueryAsync")]
        [InlineData("ExecuteScalarAsync")]
        [InlineData("ExecuteReaderAsync")]
        public async Task TestAsyncCommandExecutionEmitsTelemetry(string method)
        {
            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString =
                $"{_fixture.ConnectionString.Replace($"CLIENT_TELEMETRY_ENABLED={false}", $"CLIENT_TELEMETRY_ENABLED={true}")}poolingEnabled=false";
            await conn.OpenAsync().ConfigureAwait(false);

            var capturedActivities = new List<Activity>();
            using var listener = new ActivityListener();
            listener.ShouldListenTo = source => source.Name == ActivityStarter.ActivitySourceName;
            listener.Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData;
            listener.ActivityStopped = activity =>
            {
                if (activity.GetTagItem(TelemetryTags.SessionId)?.Equals(conn.SfSession.sessionId) == true)
                    capturedActivities.Add(activity);
            };
            ActivitySource.AddActivityListener(listener);

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            // Act
            switch (method)
            {
                case "ExecuteNonQueryAsync": await cmd.ExecuteNonQueryAsync().ConfigureAwait(false); break;
                case "ExecuteScalarAsync": await cmd.ExecuteScalarAsync().ConfigureAwait(false); break;
                case "ExecuteReaderAsync": (await cmd.ExecuteReaderAsync().ConfigureAwait(false)).Dispose(); break;
            }

            // Assert
            var expectedOp = method switch
            {
                "ExecuteNonQueryAsync" => TelemetryActivities.ExecuteNonQueryAsync,
                "ExecuteScalarAsync" => TelemetryActivities.ExecuteScalarAsync,
                "ExecuteReaderAsync" => TelemetryActivities.ExecuteDbDataReaderAsync,
                _ => throw new ArgumentException(method)
            };
            SpinWait.SpinUntil(capturedActivities.Any, TimeSpan.FromSeconds(30));
            AssertSingleTelemetryActivity(capturedActivities, expectedOp);
        }

        [SFTheory]
        [InlineData("ExecuteInAsyncMode")]
        [InlineData("ExecuteAsyncInAsyncMode")]
        public async Task TestAsyncModeExecutionEmitsTelemetry(string method)
        {
            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString =
                $"{_fixture.ConnectionString.Replace($"CLIENT_TELEMETRY_ENABLED={false}", $"CLIENT_TELEMETRY_ENABLED={true}")}poolingEnabled=false";
            conn.Open();

            var capturedActivities = new List<Activity>();
            using var listener = new ActivityListener();
            listener.ShouldListenTo = source => source.Name == ActivityStarter.ActivitySourceName;
            listener.Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData;
            listener.ActivityStopped = activity =>
            {
                if (activity.GetTagItem(TelemetryTags.SessionId)?.Equals(conn.SfSession.sessionId) == true)
                    capturedActivities.Add(activity);
            };
            ActivitySource.AddActivityListener(listener);

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            // Act
            switch (method)
            {
                case "ExecuteInAsyncMode": cmd.ExecuteInAsyncMode(); break;
                case "ExecuteAsyncInAsyncMode": await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false); break;
                default: throw new ArgumentException(method);
            }

            // Assert
            var expectedOp = method switch
            {
                "ExecuteInAsyncMode" => TelemetryActivities.ExecuteInAsyncMode,
                "ExecuteAsyncInAsyncMode" => TelemetryActivities.ExecuteAsyncInAsyncMode,
                _ => throw new ArgumentException(method)
            };
            SpinWait.SpinUntil(capturedActivities.Any, TimeSpan.FromSeconds(30));
            AssertSingleTelemetryActivity(capturedActivities, expectedOp);
        }

        [SFFact]
        public async Task TestQueryIdOperationsEmitTelemetryWIthCustomEventsFromClient()
        {
            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString =
                $"{_fixture.ConnectionString.Replace($"client_telemetry_enabled=false", $"client_telemetry_enabled=true")}poolingEnabled=false";
            conn.Open();

            var capturedActivities = new List<Activity>();
            using var listener = new ActivityListener();
            listener.ShouldListenTo = source =>
                source.Name == ActivityStarter.ActivitySourceName || source.Name == ActivityStarter.ClientDefinedTelemetrySourceName;
            listener.Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData;
            listener.ActivityStopped = activity =>
            {
                if (activity.GetTagItem(TelemetryTags.SessionId)?.Equals(conn.SfSession.sessionId) == true)
                    capturedActivities.Add(activity);
            };
            ActivitySource.AddActivityListener(listener);

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            using (var activity1 = cmd.StartActivity("Custom activity!"))
            {
                activity1.AddTelemetryEvent("event1");
                using (var activity1Nested = cmd.StartActivity("Custom activity 2!"))
                {
                    activity1.AddTelemetryEvent("event2");
                    using (var activity1NestedNested = cmd.StartActivity("Custom activity 3!"))
                    {
                        activity1NestedNested.AddTelemetryEvent("event2");
                        activity1NestedNested.SetTag("custom tag?", "value!");
                        activity1Nested.SetTag("custom tag?2", "value!2");
                        activity1Nested.SetException(new AbandonedMutexException("AAAMutex Yellow Pages"));
                    }

                    activity1.SetSuccess();
                }
            }

            // Act
            await cmd.ExecuteAsyncInAsyncMode(CancellationToken.None).ConfigureAwait(false);

            // Assert
            SpinWait.SpinUntil(() => capturedActivities.Count > 3, TimeSpan.FromSeconds(30));
            Assert.Equal(4, capturedActivities.Count);
            var capturedActivity1 = capturedActivities.Single(x => x.DisplayName == "Custom activity!");
            var capturedActivity2 = capturedActivities.Single(x => x.DisplayName == "Custom activity 2!");
            var capturedActivity3 = capturedActivities.Single(x => x.DisplayName == "Custom activity 3!");

            Assert.Equal(ActivityKind.Client, capturedActivity1.Kind);
            Assert.Equal(ActivityKind.Client, capturedActivity2.Kind);
            Assert.Equal(ActivityKind.Client, capturedActivity3.Kind);

            Assert.Equal(2, capturedActivity1.Events.Count());
            Assert.Single(capturedActivity2.Events);
            Assert.Single(capturedActivity3.Events);

            Assert.Equal(conn.SfSession.sessionId, capturedActivity1.GetTagItem(TelemetryTags.SessionId));
            Assert.Equal(conn.SfSession.sessionId, capturedActivity2.GetTagItem(TelemetryTags.SessionId));
            Assert.Equal(conn.SfSession.sessionId, capturedActivity3.GetTagItem(TelemetryTags.SessionId));

            Assert.Equal("OK", capturedActivity1.GetTagItem(TelemetryTags.StatusCode));
            Assert.Equal("ERROR", capturedActivity2.GetTagItem(TelemetryTags.StatusCode));
            Assert.Null(capturedActivity3.GetTagItem(TelemetryTags.StatusCode));
        }

        [SFFact]
        public async Task TestSetQueryTagOverridesConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string expectedQueryTag = "Test QUERY_TAG 12345";
                string connectQueryTag = "Test 123";
                conn.ConnectionString = _fixture.ConnectionString + $";query_tag={connectQueryTag}";

                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();
                ((SnowflakeDbCommand)command).QueryTag = expectedQueryTag;
                // This query itself will be part of the history and will have the query tag
                command.CommandText = "SELECT QUERY_TAG FROM table(information_schema.query_history_by_session())";
                var queryTag = command.ExecuteScalar();

                Assert.Equal(expectedQueryTag, queryTag);
            }
        }

        [SFFact]
        public async Task TestCommandWithCommentEmbedded()
        {
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();

                command.CommandText = "\r\nselect '--'\r\n";
                var reader = command.ExecuteReader();

                Assert.True(reader.Read());
                Assert.Equal("--", reader.GetString(0));
            }
        }

        [SFFact]
        public async Task TestCommandWithCommentEmbeddedAsync()
        {
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();

                command.CommandText = "\r\nselect '--'\r\n";
                var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

                Assert.True(await reader.ReadAsync().ConfigureAwait(false));
                Assert.Equal("--", reader.GetString(0));
            }
        }

        [SFFact]
        public async Task TestExecuteNonQueryReturnsCorrectRowCountForUploadWithMultipleFiles()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            const int NumberOfFiles = 5;
            const int NumberOfRows = 3;
            const int ExpectedRowCount = NumberOfFiles * NumberOfRows;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

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
                            File.WriteAllText(Path.Combine(tempFolder, $"{GetType().Name}_{i}.csv"), data);
                        }
                        await _fixture.CreateOrReplaceTable(conn, tableName, new[] { "COL1 STRING" });
                        cmd.CommandText = $"PUT file://{Path.Combine(tempFolder, "*.csv")} @%{tableName} AUTO_COMPRESS=FALSE";
                        var reader = cmd.ExecuteReader();

                        // Act
                        cmd.CommandText = $"COPY INTO {tableName} FROM @%{tableName} PATTERN='.*.csv' FILE_FORMAT=(TYPE=CSV)";
                        int actualRowCount = await cmd.ExecuteNonQueryAsync();

                        // Assert
                        Assert.Equal(ExpectedRowCount, actualRowCount);
                    }
                    finally
                    {
                        Directory.Delete(tempFolder, true);
                    }
                }
            }
        }

        [SFFact]
        public async Task TestExecuteNonQueryAsyncReturnsCorrectRowCountForUploadWithMultipleFiles()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            const int NumberOfFiles = 5;
            const int NumberOfRows = 3;
            const int ExpectedRowCount = NumberOfFiles * NumberOfRows;

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None);

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
                            File.WriteAllText(Path.Combine(tempFolder, $"{GetType().Name}_{i}.csv"), data);
                        }
                        await _fixture.CreateOrReplaceTable(conn, tableName, new[] { "COL1 STRING" });
                        cmd.CommandText = $"PUT file://{Path.Combine(tempFolder, "*.csv")} @%{tableName} AUTO_COMPRESS=FALSE";
                        var reader = cmd.ExecuteReader();

                        // Act
                        cmd.CommandText = $"COPY INTO {tableName} FROM @%{tableName} PATTERN='.*.csv' FILE_FORMAT=(TYPE=CSV)";
                        int actualRowCount = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

                        // Assert
                        Assert.Equal(ExpectedRowCount, actualRowCount);
                    }
                    finally
                    {
                        Directory.Delete(tempFolder, true);
                    }
                }
            }
        }

        private static void AssertSingleTelemetryActivity(List<Activity> capturedActivities, string expectedOperationName)
        {
            var matching = capturedActivities
                .Where(a => a.OperationName == expectedOperationName)
                .ToList();

            AssertExtensions.Equal(1, matching.Count,
                $"Expected exactly 1 {expectedOperationName} activity but found {matching.Count}. " +
                $"All captured: [{string.Join(", ", capturedActivities.Select(a => a.OperationName))}]");

            var activity = matching.Single();
            Assert.Equal(ActivityKind.Client, activity.Kind);
            Assert.Equal("OK", activity.GetTagItem(TelemetryTags.StatusCode));
            Assert.Equal("snowflake", activity.GetTagItem(TelemetryTags.DbSystem));
            Assert.NotNull(activity.GetTagItem(TelemetryTags.SessionId));
            Assert.NotNull(activity.GetTagItem(TelemetryTags.DbWarehouse));
            Assert.NotNull(activity.GetTagItem(TelemetryTags.DbRole));
            Assert.NotNull(activity.GetTagItem(TelemetryTags.DbName));
        }
    }
}
