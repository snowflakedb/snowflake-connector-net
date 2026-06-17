using System;
using System.Data;
using System.Data.Common;
using System.Linq;
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
    public sealed class ConnectionMultiplePoolsAsyncIT : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;

        public ConnectionMultiplePoolsAsyncIT(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixture = fixture;
            ConnectionManagerTestsFacade.RegisterDedicatedContext(this, ConnectionPoolType.MultipleConnectionPool);
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        public void Dispose()
        {
            ConnectionManagerTestsFacade.UnregisterDedicatedContext(this);
        }

        [SFFact]
        public async Task TestAddToPoolOnOpenAsync()
        {
            // arrange
            var connection = new SnowflakeDbConnection(_fixture.ConnectionString + "minPoolSize=1;poolingEnabled=true");

            // act
            await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // cleanup
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestFailForInvalidConnectionAsync()
        {
            // arrange
            var invalidConnectionString = ";connection_timeout=123";
            var connection = new SnowflakeDbConnection(invalidConnectionString);

            // act
            try
            {
                await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Fail("OpenAsync should fail for invalid connection string");
            }
            catch
            {
            }

            var thrown = Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.GetPool(connection.ConnectionString));

            // assert
            Assert.Contains("Required property ACCOUNT is not provided", thrown.Message);
        }

        [SFFact]
        public async Task TestConnectionPoolWithInvalidOpenAsync()
        {
            // make the connection string unique so it won't pick up connection
            // pooled by other test cases.
            string connStr = _fixture.ConnectionString +
                             "minPoolSize=0;maxPoolSize=10;application=conn_pool_test_invalid_openasync2;poolingEnabled=true";
            using (var connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = connStr;
                // call openAsync but do not wait and destroy it direct
                // so the session is initialized with empty token
                _ = connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            }

            // use the same connection string to make a new connection
            // to ensure the invalid connection made previously is not pooled
            using (var connection1 = new SnowflakeDbConnection())
            {
                connection1.ConnectionString = connStr;
                // this will not open a new session but get the invalid connection from pool
                await connection1.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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

        [SFFact]
        public async Task TestMinPoolSizeAsync()
        {
            // arrange
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = _fixture.ConnectionString + "application=TestMinPoolSizeAsync;minPoolSize=3;poolingEnabled=true";

            // act
            await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            await Awaiter.WaitUntilConditionOrTimeout(() => pool.GetCurrentPoolSize() == 3, TimeSpan.FromMilliseconds(1000)).ConfigureAwait(false);
            Assert.Equal(3, pool.GetCurrentPoolSize());

            // cleanup
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "minPoolSize=0;poolingEnabled=true";
            var connection = new SnowflakeDbConnection(connectionString);
            await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        public async Task TestReleaseConnectionWhenRollbackFailsAsync()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "minPoolSize=0;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var commandThrowingExceptionOnlyForRollback = MockHelper.CommandThrowingExceptionOnlyForRollback();
            var mockDbProviderFactory = new Mock<DbProviderFactory>();
            mockDbProviderFactory.Setup(p => p.CreateCommand()).Returns(commandThrowingExceptionOnlyForRollback.Object);
            Assert.Equal(0, pool.GetCurrentPoolSize());
            var connection = new TestSnowflakeDbConnection(mockDbProviderFactory.Object);
            connection.ConnectionString = connectionString;
            await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            await connection.BeginTransactionAsync();
            Assert.True(connection.HasActiveExplicitTransaction());

            // act
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact(DisplayName = "test connection pooling with concurrent connection using async calls")]
        public async Task TestConcurrentConnectionPoolingAsync()
        {
            // add test case name in connection string to make in unique for each test case
            // set short expiration timeout to cover the case that connection expired
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPoolingAsync2;ExpirationTimeout=3;poolingEnabled=true";
            await ConnectionSinglePoolCacheAsyncIT.ConcurrentPoolingAsyncHelper(connStr, true, 5, 5, 3);
        }

        [SFFact(DisplayName = "test connection pooling with concurrent connection and using async calls no close call for connection. Connection is closed when Dispose() is called by framework.")]
        public async Task TestConcurrentConnectionPoolingDisposeAsync()
        {
            // add test case name in connection string to make in unique for each test case
            // set short expiration timeout to cover the case that connection expired
            string connStr = _fixture.ConnectionString +
                             ";application=TestConcurrentConnectionPoolingDisposeAsync2;ExpirationTimeout=3;poolingEnabled=true";
            await ConnectionSinglePoolCacheAsyncIT.ConcurrentPoolingAsyncHelper(connStr, false, 5, 5, 3);
        }

        [SFFact]
        public async Task TestBasicConnectionPool()
        {
            var connectionString = _fixture.ConnectionString + "minPoolSize=0;maxPoolSize=1;poolingEnabled=true";
            var conn1 = new SnowflakeDbConnection(connectionString);
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);
            await conn1.CloseAsync();

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString, null).GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestReuseSessionInConnectionPool() // old name: TestConnectionPool
        {
            var connectionString = _fixture.ConnectionString + "minPoolSize=1;poolingEnabled=true";
            var conn1 = new SnowflakeDbConnection(connectionString);
            await conn1.OpenAsync(CancellationToken);
            Assert.Equal(ConnectionState.Open, conn1.State);
            await conn1.CloseAsync();
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString;
            await conn2.OpenAsync(CancellationToken);
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());

            await conn2.CloseAsync();
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
        }

        [SFFact]
        public async Task TestReuseSessionInConnectionPoolReachingMaxConnections() // old name: TestConnectionPoolFull
        {
            var connectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connectionString;
            await conn1.OpenAsync(CancellationToken);
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString;
            await conn2.OpenAsync(CancellationToken);
            Assert.Equal(ConnectionState.Open, conn2.State);

            Assert.Equal(2, pool.GetCurrentPoolSize());
            await conn1.CloseAsync();
            await conn2.CloseAsync();
            Assert.Equal(2, pool.GetCurrentPoolSize());

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = connectionString;
            await conn3.OpenAsync(CancellationToken);
            Assert.Equal(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = connectionString;
            await conn4.OpenAsync(CancellationToken);
            Assert.Equal(ConnectionState.Open, conn4.State);

            await conn3.CloseAsync();
            Assert.Equal(2, pool.GetCurrentPoolSize());
            await conn4.CloseAsync();
            Assert.Equal(2, pool.GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
            Assert.Equal(ConnectionState.Closed, conn4.State);
        }

        [SFFact]
        public async Task TestWaitForTheIdleConnectionWhenExceedingMaxConnectionsLimitAsync()
        {
            // arrange
            var connectionString = _fixture.ConnectionString +
                                   "application=TestWaitForMaxSize2;waitingForIdleSessionTimeout=1s;maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());
            var conn1 = await OpenConnectionAsync(connectionString);
            var conn2 = await OpenConnectionAsync(connectionString);
            var watch = new StopWatch();

            // act
            watch.Start();
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => OpenConnectionAsync(connectionString)).ConfigureAwait(false);
            watch.Stop();

            // assert
            Assert.Contains("Unable to connect", thrown.Message);
            Assert.True(thrown.InnerException is AggregateException);
            var nestedException = ((AggregateException)thrown.InnerException).InnerException;
            Assert.Contains("Could not obtain a connection from the pool within a given timeout", nestedException.Message);
            Assert.InRange(watch.ElapsedMilliseconds, 1000, long.MaxValue);
            Assert.Equal(2, pool.GetCurrentPoolSize());

            // cleanup
            await conn1.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            await conn2.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestBusyAndIdleConnectionsCountedInPoolSize()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;

            // act
            await connection.OpenAsync(CancellationToken);

            // assert
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // act
            await connection.CloseAsync();

            // assert
            Assert.Equal(1, pool.GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestConnectionPoolDisable()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString + ";poolingEnabled=false");
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            // act
            await conn1.OpenAsync(CancellationToken);

            // assert
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            await conn1.CloseAsync();

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestNewConnectionPoolClean()
        {
            var connectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true;";
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connectionString;
            await conn1.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString + "retryCount=1";
            await conn2.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn2.State);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = connectionString + "retryCount=2";
            await conn3.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn3.State);

            await conn1.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            await conn2.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(conn2.ConnectionString).GetCurrentPoolSize());
            SnowflakeDbConnectionPool.ClearAllPools();
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(conn2.ConnectionString).GetCurrentPoolSize());
            await conn3.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(conn3.ConnectionString).GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
        }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        public async Task TestConnectionPoolExpirationWorks()
        {
            // arrange
            const int ExpirationTimeoutInSeconds = 10;
            var connectionString = _fixture.ConnectionString + $"expirationTimeout={ExpirationTimeoutInSeconds};maxPoolSize=4;minPoolSize=2;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPoolInternal(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            var conn1 = await OpenConnectionAsync(connectionString);
            var conn2 = await OpenConnectionAsync(connectionString);
            var conn3 = await OpenConnectionAsync(connectionString);
            var conn4 = await OpenConnectionAsync(connectionString);

            // assert
            Assert.Equal(4, pool.GetCurrentPoolSize());

            // act
            await WaitUntilAllSessionsCreatedOrTimeout(pool);
            var beforeSleepMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await Task.Delay(TimeSpan.FromSeconds(ExpirationTimeoutInSeconds)).ConfigureAwait(false);
            await conn1.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            await conn2.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            await conn3.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            await conn4.CloseAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.Equal(2, pool.GetCurrentPoolSize()); // 2 idle sessions, but expired because close doesn't remove expired sessions

            // act
            await WaitUntilAllSessionsCreatedOrTimeout(pool);
            var conn5 = await OpenConnectionAsync(connectionString);
            await WaitUntilAllSessionsCreatedOrTimeout(pool);

            // assert
            Assert.Equal(2, pool.GetCurrentPoolSize()); // 1 idle session and 1 busy
            var sessionStartTimes = pool.GetIdleSessionsStartTimes();
            Assert.Single(sessionStartTimes);
            Assert.True(sessionStartTimes.First() > beforeSleepMillis);
            Assert.True(conn5.SfSession.GetStartTime() > beforeSleepMillis);
        }

        private static async Task WaitUntilAllSessionsCreatedOrTimeout(SessionPool pool)
        {
            var expectingToWaitAtMostForSessionCreations = TimeSpan.FromSeconds(15);
            await Awaiter.WaitUntilConditionOrTimeout(() => pool.OngoingSessionCreationsCount() == 0, expectingToWaitAtMostForSessionCreations);
        }

        private async Task<SnowflakeDbConnection> OpenConnectionAsync(string connectionString)
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;
            await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            return connection;
        }
    }
}
