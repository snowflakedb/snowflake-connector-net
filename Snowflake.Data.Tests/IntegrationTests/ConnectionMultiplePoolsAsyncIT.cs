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

        [Test]
        public void TestBasicConnectionPool()
        {
            var connectionString = ConnectionString + "minPoolSize=0;maxPoolSize=1;poolingEnabled=true";
            var conn1 = new SnowflakeDbConnection(connectionString);
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            // assert
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(connectionString, null).GetCurrentPoolSize());
        }

        [Test]
        public void TestReuseSessionInConnectionPool() // old name: TestConnectionPool
        {
            var connectionString = ConnectionString + "minPoolSize=1;poolingEnabled=true";
            var conn1 = new SnowflakeDbConnection(connectionString);
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString;
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());

            conn2.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
        }

        [Test]
        public void TestReuseSessionInConnectionPoolReachingMaxConnections() // old name: TestConnectionPoolFull
        {
            var connectionString = ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString;
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);

            Assert.AreEqual(2, pool.GetCurrentPoolSize());
            conn1.Close();
            conn2.Close();
            Assert.AreEqual(2, pool.GetCurrentPoolSize());

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = connectionString;
            conn3.Open();
            Assert.AreEqual(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = connectionString;
            conn4.Open();
            Assert.AreEqual(ConnectionState.Open, conn4.State);

            conn3.Close();
            Assert.AreEqual(2, pool.GetCurrentPoolSize());
            conn4.Close();
            Assert.AreEqual(2, pool.GetCurrentPoolSize());

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            Assert.AreEqual(ConnectionState.Closed, conn3.State);
            Assert.AreEqual(ConnectionState.Closed, conn4.State);
        }

        [Test]
        public void TestWaitForTheIdleConnectionWhenExceedingMaxConnectionsLimitAsync()
        {
            // arrange
            var connectionString = ConnectionString + "application=TestWaitForMaxSize2;waitingForIdleSessionTimeout=1s;maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "expecting pool to be empty");
            var conn1 = OpenConnection(connectionString);
            var conn2 = OpenConnection(connectionString);
            var watch = new StopWatch();

            // act
            watch.Start();
            var thrown = Assert.ThrowsAsync<SnowflakeDbException>(() => OpenConnectionAsync(connectionString));
            watch.Stop();

            // assert
            Assert.That(thrown.Message, Does.Contain("Unable to connect"));
            Assert.IsTrue(thrown.InnerException is AggregateException);
            var nestedException = ((AggregateException)thrown.InnerException).InnerException;
            Assert.That(nestedException.Message, Does.Contain("Could not obtain a connection from the pool within a given timeout"));
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(1000, 1500));
            Assert.AreEqual(pool.GetCurrentPoolSize(), 2);

            // cleanup
            conn1.Close();
            conn2.Close();
        }

        [Test]
        public void TestBusyAndIdleConnectionsCountedInPoolSize()
        {
            // arrange
            var connectionString = ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;

            // act
            connection.Open();

            // assert
            Assert.AreEqual(1, pool.GetCurrentPoolSize());

            // act
            connection.Close();

            // assert
            Assert.AreEqual(1, pool.GetCurrentPoolSize());
        }

             [Test]
        public void TestConnectionPoolDisable()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(ConnectionString + ";poolingEnabled=false");
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;

            // act
            conn1.Open();

            // assert
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            Assert.AreEqual(0, pool.GetCurrentPoolSize());

            // act
            conn1.Close();

            // assert
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
        }

        [Test]
        public void TestNewConnectionPoolClean()
        {
            var connectionString = ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true;";
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString + "retryCount=1";
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = connectionString + "retryCount=2";
            conn3.Open();
            Assert.AreEqual(ConnectionState.Open, conn3.State);

            conn1.Close();
            conn2.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(conn2.ConnectionString).GetCurrentPoolSize());
            SnowflakeDbConnectionPool.ClearAllPools();
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetPool(conn2.ConnectionString).GetCurrentPoolSize());
            conn3.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(conn3.ConnectionString).GetCurrentPoolSize());

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            Assert.AreEqual(ConnectionState.Closed, conn3.State);
        }

        [Test]
        [Retry(3)]
        public void TestConnectionPoolExpirationWorks()
        {
            // arrange
            const int ExpirationTimeoutInSeconds = 1;
            var connectionString = ConnectionString + $"expirationTimeout={ExpirationTimeoutInSeconds};maxPoolSize=4;minPoolSize=2;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPoolInternal(connectionString);
            Assert.AreEqual(0, pool.GetCurrentPoolSize());

            // act
            var conn1 = OpenConnection(connectionString);
            var conn2 = OpenConnection(connectionString);
            var conn3 = OpenConnection(connectionString);
            var conn4 = OpenConnection(connectionString);

            // assert
            Assert.AreEqual(4, pool.GetCurrentPoolSize());

            // act
            WaitUntilAllSessionsCreatedOrTimeout(pool);
            var beforeSleepMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Thread.Sleep(TimeSpan.FromSeconds(ExpirationTimeoutInSeconds));
            conn1.Close();
            conn2.Close();
            conn3.Close();
            conn4.Close();

            // assert
            Assert.AreEqual(2, pool.GetCurrentPoolSize()); // 2 idle sessions, but expired because close doesn't remove expired sessions

            // act
            WaitUntilAllSessionsCreatedOrTimeout(pool);
            var conn5 = OpenConnection(connectionString);
            WaitUntilAllSessionsCreatedOrTimeout(pool);

            // assert
            Assert.AreEqual(2, pool.GetCurrentPoolSize()); // 1 idle session and 1 busy
            var sessionStartTimes = pool.GetIdleSessionsStartTimes();
            Assert.AreEqual(1, sessionStartTimes.Count);
            Assert.That(sessionStartTimes.First(), Is.GreaterThan(beforeSleepMillis));
            Assert.That(conn5.SfSession.GetStartTime(), Is.GreaterThan(beforeSleepMillis));
        }

        private void WaitUntilAllSessionsCreatedOrTimeout(SessionPool pool)
                {
                    var expectingToWaitAtMostForSessionCreations = TimeSpan.FromSeconds(15);
                    Awaiter.WaitUntilConditionOrTimeout(() => pool.OngoingSessionCreationsCount() == 0, expectingToWaitAtMostForSessionCreations);
                }

        private async Task<SnowflakeDbConnection> OpenConnectionAsync(string connectionString)
                {
                    var connection = new SnowflakeDbConnection();
                    connection.ConnectionString = connectionString;
                    await connection.OpenAsync().ConfigureAwait(false);
                    return connection;
                }

        private SnowflakeDbConnection OpenConnection(string connectionString)
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;
            connection.Open();
            return connection;
        }
    }
}
