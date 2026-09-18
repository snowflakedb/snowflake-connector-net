using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Snowflake.Data.Tests.Util.Shims;
using Xunit;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class SFDbCommandIT : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFDbCommandIT(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        [SFFact]
        public void TestDataSourceError()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

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
                    Assert.Equal(2003, e.ErrorCode);
                    Assert.NotEqual("", e.QueryId);
                }

                conn.Close();
            }
        }

        [Collection(nameof(SfDbCommandIsolatedFixture))]
        public sealed class Isolated : SFBaseTestAsync
        {
            private readonly SFBaseTestAsyncFixture _isolatedFixture;

            [CollectionDefinition(nameof(SfDbCommandIsolatedFixture), DisableParallelization = true)]
            public sealed class SfDbCommandIsolatedFixture : ICollectionFixture<SfDbCommandIsolatedFixture>
            {
            }

            public Isolated(SFBaseTestAsyncFixture fixture) : base(fixture)
            {
                _isolatedFixture = fixture;
            }

            [SFFact]
            public async Task TestCancelQuery()
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _isolatedFixture.ConnectionString + "poolingEnabled=false";

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

                    await Task.Delay(8000).ConfigureAwait(false);
                    cmd.Cancel();

                    try
                    {
                        executionThread.Wait();
                    }
                    catch (AggregateException e)
                    {
                        Assert.IsAssignableFrom<TaskCanceledException>(e.InnerException);
                    }

                    conn.Close();
                }
            }

            [SFFact(RetriesCount = RetriesCount.Once)]
            public async Task TestExternalCancellationPropagatesDuringBindStageUpload()
            {
                var tableName = _isolatedFixture.TableNameBaseName + Guid.NewGuid().ToString("N");
                using var conn = new SnowflakeDbConnection();
                conn.ConnectionString = _isolatedFixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                await _isolatedFixture.CreateOrReplaceTable(conn, tableName, [
                    "cola INTEGER"
                ]).ConfigureAwait(false);

                // Phase 1 – prime the bind-stage cache so CreateStageAsync is a no-op later.
                using var prepCmd = conn.CreateCommand();
                prepCmd.CommandText = $"insert into {tableName} values (?)";
                var prepParam = prepCmd.CreateParameter();
                prepParam.ParameterName = "1";
                prepParam.DbType = DbType.Int16;
                prepParam.Value = Enumerable.Range(0, 2).ToArray();
                prepCmd.Parameters.Add(prepParam);

                conn.SfSession.ParameterMap[SFSessionParameter.CLIENT_STAGE_ARRAY_BINDING_THRESHOLD] = "1";
                await prepCmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);

                // Phase 2 – large insert, cancel quickly
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"insert into {tableName} values (?)";
                cmd.CommandTimeout = 0; // no timeout – rely solely on external token

                var param = cmd.CreateParameter();
                param.ParameterName = "1";
                param.DbType = DbType.Int16;
                param.Value = Enumerable.Range(0, 5_000_000).ToArray();
                cmd.Parameters.Add(param);

                const int CancelDelaySeconds = 3;
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(CancelDelaySeconds));

                var sw = Stopwatch.StartNew();
                var thrown = await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                    await cmd.ExecuteNonQueryAsync(cts.Token).ConfigureAwait(false)
                ).ConfigureAwait(false);
                sw.Stop();

                // The inner exception should indicate query-cancelled (external token).
                var detail = Assert.IsType<SnowflakeDbException>(thrown.InnerException);
                Assert.Equal(SFError.QUERY_CANCELLED.GetAttribute<SFErrorAttr>().errorCode, detail.ErrorCode);

                // With the fix the operation cancels promptly after the token fires.
                // Without the fix the PUT upload runs to completion first, adding many
                // seconds.  A generous threshold avoids CI flakiness while still catching
                // the regression where the token is silently dropped.
                Assert.True(sw.Elapsed.TotalSeconds < CancelDelaySeconds + 30,
                    $"Expected cancellation to propagate into the stage PUT upload. " +
                    $"Elapsed: {sw.Elapsed.TotalSeconds:F1}s, CancelAfter: {CancelDelaySeconds}s.");

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestExecAPI()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(ConnectionState.Open, conn.State);

                using (DbCommand cmd = conn.CreateCommand())
                {
                    long queryResult = 0;
                    cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 3)) v";
                    using var reader = cmd.ExecuteReader();
                    Assert.True(reader.Read());
                    queryResult = reader.GetInt64(0);
                    Assert.False(reader.Read());
                    Assert.NotEqual(0, queryResult);
                }

                await conn.CloseAsync().ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestExecParallelAPI()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";

                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.Equal(ConnectionState.Open, conn.State);

                Task[] taskArray = new Task[5];
                for (int i = 0; i < taskArray.Length; i++)
                {
                    taskArray[i] = Task.Factory.StartNew(() =>
                    {
                        using (DbCommand cmd = conn.CreateCommand())
                        {
                            long queryResult = 0;
                            cmd.CommandText = "select count(seq4()) from table(generator(timelimit => 3)) v";
                            using var reader = cmd.ExecuteReader();
                            Assert.True(reader.Read());
                            queryResult = reader.GetInt64(0);
                            Assert.False(reader.Read());
                            Assert.NotEqual(0, queryResult);
                        }
                    });
                }
                Task.WaitAll(taskArray);
                await conn.CloseAsync().ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetStatusOfInvalidQueryId()
        {
            string fakeQueryId = "fakeQueryId";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.Throws<Exception>(() => cmd.GetQueryStatus(fakeQueryId));

                    // Assert
                    Assert.Contains("Invalid query id format. Expected a UUID.", thrown.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetResultsOfInvalidQueryId()
        {
            string fakeQueryId = "fakeQueryId";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.Throws<AggregateException>(() => cmd.GetResultsFromQueryId(fakeQueryId));

                    // Assert
                    Assert.Contains("Invalid query id format. Expected a UUID.", thrown.InnerException.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetStatusOfUnknownQueryId()
        {
            string unknownQueryId = "ab123cde-1cba-789a-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var queryStatus = cmd.GetQueryStatus(unknownQueryId);

                    // Assert
                    Assert.Equal(QueryStatus.NoData, queryStatus);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact(Skip = "The test takes too long to finish when using the default retry")]
        public async Task TestGetResultsOfUnknownQueryIdWithDefaultRetry()
        {
            string unknownQueryId = "ba987def-1abc-987f-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Act
                    var thrown = Assert.Throws<AggregateException>(() => cmd.GetResultsFromQueryId(unknownQueryId));

                    // Assert
                    Assert.Contains("Max retry for no data is reached", thrown.InnerException.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetResultsOfUnknownQueryIdWithConfiguredRetry()
        {
            var queryResultsRetryCount = 3;
            var queryResultsRetryPattern = new int[] { 1, 2 };
            var unknownQueryId = "ba987def-1abc-987f-987f-1234a56b789c";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (SnowflakeDbCommand cmd = (SnowflakeDbCommand)conn.CreateCommand())
                {
                    // Arrange
                    QueryResultsAwaiter queryResultsAwaiter =
                        new QueryResultsAwaiter(new QueryResultsRetryConfig(queryResultsRetryCount, queryResultsRetryPattern));
                    var task = queryResultsAwaiter.RetryUntilQueryResultIsAvailable(conn, unknownQueryId, CancellationToken.None, false);

                    // Act
                    var thrown = Assert.Throws<AggregateException>(() => task.Wait());

                    // Assert
                    Assert.Contains("Max retry for no data is reached", thrown.InnerException.Message);
                }

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }
}
