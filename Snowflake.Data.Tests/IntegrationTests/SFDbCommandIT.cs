using System;
using System.Data;
using System.Data.Common;
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
