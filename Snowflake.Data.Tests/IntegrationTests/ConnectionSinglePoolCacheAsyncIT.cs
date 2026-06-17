using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class ConnectionSinglePoolCacheAsyncIT : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;

        public ConnectionSinglePoolCacheAsyncIT(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixture = fixture;
            ConnectionManagerTestsFacade.RegisterDedicatedContext(this, ConnectionPoolType.SingleConnectionCache);
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        public void Dispose()
        {
            ConnectionManagerTestsFacade.UnregisterDedicatedContext(this);
        }

        [SFFact]
        public async Task TestPutConnectionToPoolOnCloseAsync()
        {
            // arrange
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                Assert.Equal(ConnectionState.Closed, conn.State);
                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                await conn.OpenAsync(connectionCancelToken.Token).ConfigureAwait(false);

                // act
                await conn.CloseAsync(connectionCancelToken.Token).ConfigureAwait(false);

                // assert
                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            }
        }

        [SFFact]
        public async Task TestDoNotPutInvalidConnectionToPoolAsync()
        {
            // arrange
            var invalidConnectionString = ";connection_timeout=0";
            using (var conn = new SnowflakeDbConnection(invalidConnectionString))
            {
                Assert.Equal(ConnectionState.Closed, conn.State);
                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                try
                {
                    await conn.OpenAsync(connectionCancelToken.Token).ConfigureAwait(false);
                    Assert.Fail("OpenAsync should throw exception");
                }
                catch { }

                // act
                await conn.CloseAsync(connectionCancelToken.Token).ConfigureAwait(false);

                // assert
                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            }
        }

        [SFFact]
        public async Task TestConnectionPoolWithInvalidOpenAsync()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            // make the connection string unique so it won't pick up connection
            // pooled by other test cases.
            string connStr = _fixture.ConnectionString + "application=conn_pool_test_invalid_openasync";
            using (var connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = connStr;
                // call openAsync but do not wait and destroy it direct
                // so the session is initialized with empty token
                connection.OpenAsync();
            }

            // use the same connection string to make a new connection
            // to ensure the invalid connection made previously is not pooled
            using (var connection1 = new SnowflakeDbConnection())
            {
                connection1.ConnectionString = connStr;
                // this will not open a new session but get the invalid connection from pool
                connection1.Open();
                // Now run query with connection1
                var command = connection1.CreateCommand();
                command.CommandText = "select 1, 2, 3";

                try
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                // Process each column as appropriate
                                reader.GetFieldValue<object>(i);
                            }
                        }
                    }
                }
                catch (SnowflakeDbException)
                {
                    // fail the test case if anything wrong.
                    Assert.Fail();
                }
            }
        }

        [SFFact(DisplayName = "test connection pooling with concurrent connection using async calls")]
        public async Task TestConcurrentConnectionPoolingAsync()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPoolingAsync";
            SnowflakeDbConnectionPool.SetMaxPoolSize(3);
            SnowflakeDbConnectionPool.SetTimeout(1); // set short pooling timeout to cover the case that connection expired
            await ConcurrentPoolingAsyncHelper(connStr, true, 5, 5, 3);
            SnowflakeDbConnectionPool.SetTimeout(3600);
        }

        [SFFact(DisplayName = "test connection pooling with concurrent connection and using async calls no close call for connection. Connection is closed when Dispose() is called by framework.")]
        public async Task TestConcurrentConnectionPoolingDisposeAsync()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPoolingDisposeAsync";
            SnowflakeDbConnectionPool.SetMaxPoolSize(3);
            SnowflakeDbConnectionPool.SetTimeout(1); // set short pooling timeout to cover the case that connection expired
            await ConcurrentPoolingAsyncHelper(connStr, false, 5, 5, 3);
            SnowflakeDbConnectionPool.SetTimeout(3600);
        }

        [SFFact]
        public async Task TestBasicConnectionPool()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);

            var conn1 = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);
            await conn1.CloseAsync(CancellationToken.None);

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestPoolContainsClosedConnections() // old name: TestConnectionPool
        {
            var conn1 = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);
            await conn1.CloseAsync(CancellationToken.None);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString;
            await conn2.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());

            await conn2.CloseAsync(CancellationToken.None);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
        }

        [SFFact]
        public async Task TestPoolContainsAtMostMaxPoolSizeConnections() // old name: TestConnectionPoolFull
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString + " retryCount=1";
            await conn2.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            await conn1.CloseAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString + "  retryCount=2";
            await conn3.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = _fixture.ConnectionString + "  retryCount=3";
            await conn4.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn4.State);

            await conn3.CloseAsync(CancellationToken.None);
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            await conn4.CloseAsync(CancellationToken.None);
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
            Assert.Equal(ConnectionState.Closed, conn4.State);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [SFFact]
        public async Task TestConnectionPoolDisableFromPoolManagerLevel()
        {
            // arrange
            SnowflakeDbConnectionPool.SetPooling(false);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            // act
            await conn1.OpenAsync(CancellationToken.None);

            // assert
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            // act
            await conn1.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestConnectionPoolDisable()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString);
            SnowflakeDbConnectionPool.SetPooling(false);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            // act
            await conn1.OpenAsync(CancellationToken.None);

            // assert
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            await conn1.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestConnectionPoolClean()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString + " retryCount=1";
            await conn2.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn2.State);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString + "  retryCount=2";
            await conn3.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn3.State);

            await conn1.CloseAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            SnowflakeDbConnectionPool.ClearAllPools();
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            await conn3.CloseAsync(CancellationToken.None);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
        }

        [SFFact]
        public async Task TestConnectionPoolExpirationWorks()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetTimeout(10);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            await conn1.OpenAsync(CancellationToken.None);
            await conn1.CloseAsync(CancellationToken.None);
            SnowflakeDbConnectionPool.SetTimeout(0);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString;
            await conn2.OpenAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString;
            await conn3.OpenAsync(CancellationToken.None);
            await conn3.CloseAsync(CancellationToken.None);

            // The pooling timeout should apply to all connections being pooled,
            // not just the connections created after the new setting,
            // so expected result should be 0
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connection = new SnowflakeDbConnection(_fixture.ConnectionString);
            await connection.OpenAsync(CancellationToken.None);
            var pool = SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            await connection.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestReleaseConnectionWhenRollbackFails()
        {
            // arrange
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            var commandThrowingExceptionOnlyForRollback = MockHelper.CommandThrowingExceptionOnlyForRollback();
            var mockDbProviderFactory = new Mock<DbProviderFactory>();
            mockDbProviderFactory.Setup(p => p.CreateCommand()).Returns(commandThrowingExceptionOnlyForRollback.Object);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            var connection = new TestSnowflakeDbConnection(mockDbProviderFactory.Object);
            connection.ConnectionString = _fixture.ConnectionString;
            await connection.OpenAsync(CancellationToken.None);
            connection.BeginTransaction();
            Assert.True(connection.HasActiveExplicitTransaction());
            // no Rollback or Commit; during internal Rollback while closing a connection a mocked exception will be thrown

            // act
            await connection.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        public async Task TestCloseSessionAfterTimeout()
        {
            // arrange
            const int SessionTimeoutSeconds = 1;
            const int TimeForBackgroundSessionCloseMillis = 3000;
            SnowflakeDbConnectionPool.SetTimeout(SessionTimeoutSeconds);
            var conn1 = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn1.OpenAsync(CancellationToken.None);
            var session = conn1.SfSession;
            await conn1.CloseAsync(CancellationToken.None);
            Assert.True(session.IsEstablished());
            await Task.Delay(SessionTimeoutSeconds * 1000).ConfigureAwait(false); // wait until the session is expired
            var conn2 = new SnowflakeDbConnection(_fixture.ConnectionString);

            // act
            await conn2.OpenAsync(CancellationToken.None); // it gets a session from the caching pool firstly closing session of conn1 in background

            await Awaiter.WaitUntilConditionOrTimeout(() => !session.IsEstablished(), TimeSpan.FromMilliseconds(TimeForBackgroundSessionCloseMillis)).ConfigureAwait(false);

            // assert
            Assert.False(session.IsEstablished());

            // cleanup
            await conn2.CloseAsync(CancellationToken.None);
        }

        public static Task ConcurrentPoolingAsyncHelper(string connectionString, bool closeConnection, int tasksCount, int connectionsInTask, int abandonedConnectionsCount)
        {
            var tasks = new Task[tasksCount + 1];
            for (int i = 0; i < tasksCount; i++)
            {
                tasks[i] = QueryExecutionTaskAsync(connectionString, closeConnection, connectionsInTask);
            }
            // cover the case of invalid sessions to ensure that won't
            // break connection pooling
            tasks[tasksCount] = InvalidConnectionTaskAsync(connectionString, abandonedConnectionsCount);
            return Task.WhenAll(tasks);
        }

        // task to execute query with new connection in a loop
        static async Task QueryExecutionTaskAsync(string connectionString, bool closeConnection, int times)
        {
            for (int i = 0; i < times; i++)
            {
                using (var conn = new SnowflakeDbConnection(connectionString))
                {
                    await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                    using (DbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1, 2, 3";
                        try
                        {
                            using (DbDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                            {
                                while (await reader.ReadAsync().ConfigureAwait(false))
                                {
                                    for (int j = 0; j < reader.FieldCount; j++)
                                    {
                                        // Process each column as appropriate
                                        await reader.GetFieldValueAsync<object>(j).ConfigureAwait(false);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Assert.Fail("Caught unexpected exception: " + e);
                        }
                    }
                    if (closeConnection)
                    {
                        await conn.CloseAsync(new CancellationTokenSource().Token).ConfigureAwait(false);
                    }
                }
            }
        }

        // task to generate invalid(not finish open) connections in a loop
        static async Task InvalidConnectionTaskAsync(string connectionString, int times)
        {
            for (int i = 0; i < times; i++)
            {
                using (var conn = new SnowflakeDbConnection(connectionString))
                {
                    // intentionally not using await so the connection
                    // will be disposed with invalid underlying session
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    conn.OpenAsync(CancellationToken.None);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                }

                // wait 100ms each time so the invalid sessions are generated
                // roughly at the same speed as connections for query tasks
                await Task.Delay(100).ConfigureAwait(false);
            }
        }
    }
}
