using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [NonParallelizable]
    public class ConnectionMultiplePoolsAsyncIT : SFBaseTestAsync
    {
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();
        private readonly SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [TearDown]
        public new void AfterTest()
        {
            _previousPoolConfig.Reset();
        }

        [Test]
        public async Task TestAddToPoolOnOpenAsync()
        {
            // arrange
            var connection = new SnowflakeDbConnection(ConnectionString + "minPoolSize=1;poolingEnabled=true");

            // act
            await connection.OpenAsync().ConfigureAwait(false);

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            Assert.AreEqual(1, pool.GetCurrentPoolSize());

            // cleanup
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }

        [Test]
        public async Task TestFailForInvalidConnectionAsync()
        {
            // arrange
            var invalidConnectionString = ";connection_timeout=123";
            var connection = new SnowflakeDbConnection(invalidConnectionString);

            // act
            try
            {
                await connection.OpenAsync().ConfigureAwait(false);
                Assert.Fail("OpenAsync should fail for invalid connection string");
            }
            catch { }
            var thrown = Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.GetPool(connection.ConnectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain("Required property ACCOUNT is not provided"));
        }

        [Test]
        public void TestConnectionPoolWithInvalidOpenAsync()
        {
            // make the connection string unique so it won't pick up connection
            // pooled by other test cases.
            string connStr = ConnectionString + "minPoolSize=0;maxPoolSize=10;application=conn_pool_test_invalid_openasync2;poolingEnabled=true";
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
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
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

        [Test]
        public async Task TestMinPoolSizeAsync()
        {
            // arrange
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = ConnectionString + "application=TestMinPoolSizeAsync;minPoolSize=3;poolingEnabled=true";

            // act
            await connection.OpenAsync().ConfigureAwait(false);

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            Awaiter.WaitUntilConditionOrTimeout(() => pool.GetCurrentPoolSize() == 3, TimeSpan.FromMilliseconds(1000));
            Assert.AreEqual(3, pool.GetCurrentPoolSize());

            // cleanup
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }

        [Test]
        public async Task TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connectionString = ConnectionString + "minPoolSize=0;poolingEnabled=true";
            var connection = new SnowflakeDbConnection(connectionString);
            await connection.OpenAsync().ConfigureAwait(false);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(1, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
        }

        [Test]
        public async Task TestReleaseConnectionWhenRollbackFailsAsync()
        {
            // arrange
            var connectionString = ConnectionString + "minPoolSize=0;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var commandThrowingExceptionOnlyForRollback = MockHelper.CommandThrowingExceptionOnlyForRollback();
            var mockDbProviderFactory = new Mock<DbProviderFactory>();
            mockDbProviderFactory.Setup(p => p.CreateCommand()).Returns(commandThrowingExceptionOnlyForRollback.Object);
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
            var connection = new TestSnowflakeDbConnection(mockDbProviderFactory.Object);
            connection.ConnectionString = connectionString;
            await connection.OpenAsync().ConfigureAwait(false);
            connection.BeginTransaction(); // not using async version because it is not available on .net framework
            Assert.AreEqual(true, connection.HasActiveExplicitTransaction());

            // act
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "Should not return connection to the pool");
        }

        [Test(Description = "test connection pooling with concurrent connection using async calls")]
        public void TestConcurrentConnectionPoolingAsync()
        {
            // add test case name in connection string to make in unique for each test case
            // set short expiration timeout to cover the case that connection expired
            string connStr = ConnectionString + ";application=TestConcurrentConnectionPoolingAsync2;ExpirationTimeout=3;poolingEnabled=true";
            ConnectionSinglePoolCacheAsyncIT.ConcurrentPoolingAsyncHelper(connStr, true, 5, 5, 3);
        }

        [Test(Description = "test connection pooling with concurrent connection and using async calls no close call for connection. Connection is closed when Dispose() is called by framework.")]
        public void TestConcurrentConnectionPoolingDisposeAsync()
        {
            // add test case name in connection string to make in unique for each test case
            // set short expiration timeout to cover the case that connection expired
            string connStr = ConnectionString + ";application=TestConcurrentConnectionPoolingDisposeAsync2;ExpirationTimeout=3;poolingEnabled=true";
            ConnectionSinglePoolCacheAsyncIT.ConcurrentPoolingAsyncHelper(connStr, false, 5, 5, 3);
        }
    }
}
