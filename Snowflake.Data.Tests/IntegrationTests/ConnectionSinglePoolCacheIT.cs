using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
using Moq;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class ConnectionSinglePoolCacheITFixture : IDisposable
    {
        public void Dispose()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }
    }

    [Collection(SequentialIntegrationCollection.SequentialIntegrationCollectionName)]
    public class ConnectionSinglePoolCacheIT : SFBaseTestAsync, IClassFixture<ConnectionSinglePoolCacheITFixture>, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();

        public ConnectionSinglePoolCacheIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture, ConnectionSinglePoolCacheITFixture classFixture) : base(fixture, envFixture)
        {
            _fixture = fixture;
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetPooling(true);
        }

        public void Dispose()
        {
            _previousPoolConfig.Reset();
        }

        [Fact]
        public async Task TestBasicConnectionPool()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);

            var conn1 = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn1.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn1.State);
            await conn1.CloseAsync();

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
        }

        [Fact]
        public async Task TestConcurrentConnectionPooling()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPooling;";
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            ConcurrentPoolingHelper(connStr, true);
        }

        [Fact]
        // test connection pooling with concurrent connection and no close
        // call for connection. Connection is closed when Dispose() is called
        // by framework.
        public async Task TestConcurrentConnectionPoolingDispose()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPoolingNoClose;";
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            ConcurrentPoolingHelper(connStr, false);
        }

        static void ConcurrentPoolingHelper(string connectionString, bool closeConnection)
        {
            // thread number a bit larger than pool size so some connections
            // would fail on pooling while some connections could success
            const int ThreadNum = 3;
            // set short pooling timeout to cover the case that connection expired
            const int PoolTimeout = 1;

            // reset to default settings in case it changed by other test cases
            Assert.Equal(true, SnowflakeDbConnectionPool.GetPool(connectionString).GetPooling()); // to instantiate pool
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetTimeout(PoolTimeout);

            var threads = new Task[ThreadNum];
            for (int i = 0; i < ThreadNum; i++)
            {
                threads[i] = Task.Factory.StartNew(() =>
                {
                    QueryExecutionThread(connectionString, closeConnection);
                });
            }
            Task.WaitAll(threads);
        }

        // thead to execute query with new connection in a loop
        static async Task QueryExecutionThread(string connectionString, bool closeConnection)
        {
            for (int i = 0; i < 10; i++)
            {
                using (var conn = new SnowflakeDbConnection(connectionString))
                {
                    await conn.OpenAsync();
                    using (DbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1, 2, 3";
                        try
                        {
                            using (var reader = await cmd.ExecuteReaderAsync())
                            {
                                while (await reader.ReadAsync())
                                {
                                    for (int j = 0; j < reader.FieldCount; j++)
                                    {
                                        // Process each column as appropriate
                                        reader.GetFieldValue<object>(j);
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
                        await conn.CloseAsync();
                    }
                }
            }
        }

        [Fact]
        public async Task TestPoolContainsClosedConnections() // old name: TestConnectionPool
        {
            var conn1 = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn1.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn1.State);
            await conn1.CloseAsync();
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString;
            await conn2.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());

            await conn2.CloseAsync();
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
        }

        [Fact]
        public async Task TestPoolContainsAtMostMaxPoolSizeConnections() // old name: TestConnectionPoolFull
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;
            await conn1.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString + " retryCount=1";
            await conn2.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            await conn1.CloseAsync();
            await conn2.CloseAsync();
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString + "  retryCount=2";
            await conn3.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = _fixture.ConnectionString + "  retryCount=3";
            await conn4.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn4.State);

            await conn3.CloseAsync();
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            await conn4.CloseAsync();
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
            Assert.Equal(ConnectionState.Closed, conn4.State);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Fact]
        public async Task TestConnectionPoolDisableFromPoolManagerLevel()
        {
            // arrange
            SnowflakeDbConnectionPool.SetPooling(false);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            // act
            await conn1.OpenAsync();

            // assert
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            // act
            await conn1.CloseAsync();

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Fact]
        public async Task TestConnectionPoolDisable()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString);
            SnowflakeDbConnectionPool.SetPooling(false);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            // act
            await conn1.OpenAsync();

            // assert
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            await conn1.CloseAsync();

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [Fact]
        public async Task TestConnectionPoolClean()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;
            await conn1.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString + " retryCount=1";
            await conn2.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn2.State);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString + "  retryCount=2";
            await conn3.OpenAsync();
            Assert.Equal(ConnectionState.Open, conn3.State);

            await conn1.CloseAsync();
            await conn2.CloseAsync();
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            SnowflakeDbConnectionPool.ClearAllPools();
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            await conn3.CloseAsync();
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
        }

        [Fact]
        public async Task TestConnectionPoolExpirationWorks()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetTimeout(10);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            await conn1.OpenAsync();
            await conn1.CloseAsync();
            SnowflakeDbConnectionPool.SetTimeout(0);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString;
            await conn2.OpenAsync();
            await conn2.CloseAsync();

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString;
            await conn3.OpenAsync();
            await conn3.CloseAsync();

            // The pooling timeout should apply to all connections being pooled,
            // not just the connections created after the new setting,
            // so expected result should be 0
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
        }

        [Fact]
        public async Task TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connection = new SnowflakeDbConnection(_fixture.ConnectionString);
            await connection.OpenAsync();
            var pool = SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            await connection.CloseAsync();

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [Fact]
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
            await connection.OpenAsync();
            connection.BeginTransaction();
            Assert.Equal(true, connection.HasActiveExplicitTransaction());
            // no Rollback or Commit; during internal Rollback while closing a connection a mocked exception will be thrown

            // act
            await connection.CloseAsync();

            // assert
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Fact]
        public async Task TestCloseSessionAfterTimeout()
        {
            // arrange
            const int SessionTimeoutSeconds = 1;
            const int TimeForBackgroundSessionCloseMillis = 1000;
            SnowflakeDbConnectionPool.SetTimeout(SessionTimeoutSeconds);
            var conn1 = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn1.OpenAsync();
            var session = conn1.SfSession;
            await conn1.CloseAsync();
            Assert.True(session.IsEstablished());
            await Task.Delay(SessionTimeoutSeconds * 1000).ConfigureAwait(false); // wait until the session is expired
            var conn2 = new SnowflakeDbConnection(_fixture.ConnectionString);

            // act
            await conn2.OpenAsync(); // it gets a session from the caching pool firstly closing session of conn1 in background

            await Awaiter.WaitUntilConditionOrTimeout(() => !session.IsEstablished(), TimeSpan.FromMilliseconds(TimeForBackgroundSessionCloseMillis));

            // assert
            Assert.False(session.IsEstablished());

            // cleanup
            await conn2.CloseAsync();
        }
    }
}
