using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [CollectionDefinition(nameof(ConnectionMultiplePoolsAsyncITFixture), DisableParallelization = true)]
    public sealed class ConnectionMultiplePoolsAsyncITFixture : ICollectionFixture<ConnectionMultiplePoolsAsyncITFixture.Fixture>
    {
        public sealed class Fixture
        { }
    }

    [CollectionDefinition(nameof(ConnectionMultiplePoolsAsyncITFixture))]
    public sealed class ConnectionMultiplePoolsAsyncIT : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();
        private readonly SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        public ConnectionMultiplePoolsAsyncIT(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixture = fixture;
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        public void Dispose()
        {
            _previousPoolConfig.Reset();
        }

        [SFFact]
        public async Task TestAddToPoolOnOpenAsync()
        {
            // arrange
            var connection = new SnowflakeDbConnection(_fixture.ConnectionString + "minPoolSize=1;poolingEnabled=true");

            // act
            await connection.OpenAsync(CancellationToken.None);

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // cleanup
            await connection.CloseAsync(CancellationToken.None);
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
                await connection.OpenAsync(CancellationToken.None);
                Assert.Fail("OpenAsync should fail for invalid connection string");
            }
            catch { }
            var thrown = Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.GetPool(connection.ConnectionString));

            // assert
            Assert.Contains("Required property ACCOUNT is not provided", thrown.Message);
        }

        [SFFact]
        public async Task TestConnectionPoolWithInvalidOpenAsync()
        {
            // make the connection string unique so it won't pick up connection
            // pooled by other test cases.
            string connStr = _fixture.ConnectionString + "minPoolSize=0;maxPoolSize=10;application=conn_pool_test_invalid_openasync2;poolingEnabled=true";
            using (var connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = connStr;
                // call openAsync but do not wait and destroy it direct
                // so the session is initialized with empty token
                _ = connection.OpenAsync(CancellationToken.None);
            }

            // use the same connection string to make a new connection
            // to ensure the invalid connection made previously is not pooled
            using (var connection1 = new SnowflakeDbConnection())
            {
                connection1.ConnectionString = connStr;
                // this will not open a new session but get the invalid connection from pool
                await connection1.OpenAsync(CancellationToken.None);
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
            await connection.OpenAsync(CancellationToken.None);

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            await Awaiter.WaitUntilConditionOrTimeout(() => pool.GetCurrentPoolSize() == 3, TimeSpan.FromMilliseconds(1000));
            Assert.Equal(3, pool.GetCurrentPoolSize());

            // cleanup
            await connection.CloseAsync(CancellationToken.None);
        }

        [SFFact]
        public async Task TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "minPoolSize=0;poolingEnabled=true";
            var connection = new SnowflakeDbConnection(connectionString);
            await connection.OpenAsync(CancellationToken.None);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            await connection.CloseAsync(CancellationToken.None);
            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact]
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
            await connection.OpenAsync(CancellationToken.None);
            await connection.BeginTransactionAsync();
            Assert.Equal(true, connection.HasActiveExplicitTransaction());

            // act
            await connection.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [Fact(DisplayName = "test connection pooling with concurrent connection using async calls")]
        public void TestConcurrentConnectionPoolingAsync()
        {
            // add test case name in connection string to make in unique for each test case
            // set short expiration timeout to cover the case that connection expired
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPoolingAsync2;ExpirationTimeout=3;poolingEnabled=true";
            ConnectionSinglePoolCacheAsyncIT.ConcurrentPoolingAsyncHelper(connStr, true, 5, 5, 3);
        }

        [Fact(DisplayName = "test connection pooling with concurrent connection and using async calls no close call for connection. Connection is closed when Dispose() is called by framework.")]
        public void TestConcurrentConnectionPoolingDisposeAsync()
        {
            // add test case name in connection string to make in unique for each test case
            // set short expiration timeout to cover the case that connection expired
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPoolingDisposeAsync2;ExpirationTimeout=3;poolingEnabled=true";
            ConnectionSinglePoolCacheAsyncIT.ConcurrentPoolingAsyncHelper(connStr, false, 5, 5, 3);
        }
    }
}
