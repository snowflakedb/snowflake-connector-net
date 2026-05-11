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

    public class ConnectionSinglePoolCacheIT : SFBaseTest, IClassFixture<ConnectionSinglePoolCacheITFixture>, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();

        public ConnectionSinglePoolCacheIT(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture, ConnectionSinglePoolCacheITFixture classFixture) : base(fixture, envFixture)
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
        public void TestBasicConnectionPool()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);

            var conn1 = new SnowflakeDbConnection(_fixture.ConnectionString);
            conn1.Open();
            Assert.Equal(ConnectionState.Open, conn1.State);
            conn1.Close();

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
        }

        [Fact]
        public void TestConcurrentConnectionPooling()
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
        public void TestConcurrentConnectionPoolingDispose()
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
        static void QueryExecutionThread(string connectionString, bool closeConnection)
        {
            for (int i = 0; i < 10; i++)
            {
                using (DbConnection conn = new SnowflakeDbConnection(connectionString))
                {
                    conn.Open();
                    using (DbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1, 2, 3";
                        try
                        {
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
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
                        conn.Close();
                    }
                }
            }
        }

        [Fact]
        public void TestPoolContainsClosedConnections() // old name: TestConnectionPool
        {
            var conn1 = new SnowflakeDbConnection(_fixture.ConnectionString);
            conn1.Open();
            Assert.Equal(ConnectionState.Open, conn1.State);
            conn1.Close();
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString;
            conn2.Open();
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());

            conn2.Close();
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
        }

        [Fact]
        public void TestPoolContainsAtMostMaxPoolSizeConnections() // old name: TestConnectionPoolFull
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;
            conn1.Open();
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString + " retryCount=1";
            conn2.Open();
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            conn1.Close();
            conn2.Close();
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString + "  retryCount=2";
            conn3.Open();
            Assert.Equal(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = _fixture.ConnectionString + "  retryCount=3";
            conn4.Open();
            Assert.Equal(ConnectionState.Open, conn4.State);

            conn3.Close();
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            conn4.Close();
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
            Assert.Equal(ConnectionState.Closed, conn4.State);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Fact]
        public void TestConnectionPoolDisableFromPoolManagerLevel()
        {
            // arrange
            SnowflakeDbConnectionPool.SetPooling(false);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            // act
            conn1.Open();

            // assert
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            // act
            conn1.Close();

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Fact]
        public void TestConnectionPoolDisable()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString);
            SnowflakeDbConnectionPool.SetPooling(false);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            // act
            conn1.Open();

            // assert
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            conn1.Close();

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [Fact]
        public void TestConnectionPoolClean()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;
            conn1.Open();
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString + " retryCount=1";
            conn2.Open();
            Assert.Equal(ConnectionState.Open, conn2.State);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString + "  retryCount=2";
            conn3.Open();
            Assert.Equal(ConnectionState.Open, conn3.State);

            conn1.Close();
            conn2.Close();
            Assert.Equal(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            SnowflakeDbConnectionPool.ClearAllPools();
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            conn3.Close();
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
        }

        [Fact]
        public void TestConnectionPoolExpirationWorks()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetTimeout(10);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            conn1.Open();
            conn1.Close();
            SnowflakeDbConnectionPool.SetTimeout(0);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = _fixture.ConnectionString;
            conn2.Open();
            conn2.Close();

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = _fixture.ConnectionString;
            conn3.Open();
            conn3.Close();

            // The pooling timeout should apply to all connections being pooled,
            // not just the connections created after the new setting,
            // so expected result should be 0
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString).GetCurrentPoolSize());
        }

        [Fact]
        public void TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connection = new SnowflakeDbConnection(_fixture.ConnectionString);
            connection.Open();
            var pool = SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            connection.Close();

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [Fact]
        public void TestReleaseConnectionWhenRollbackFails()
        {
            // arrange
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            var commandThrowingExceptionOnlyForRollback = MockHelper.CommandThrowingExceptionOnlyForRollback();
            var mockDbProviderFactory = new Mock<DbProviderFactory>();
            mockDbProviderFactory.Setup(p => p.CreateCommand()).Returns(commandThrowingExceptionOnlyForRollback.Object);
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            var connection = new TestSnowflakeDbConnection(mockDbProviderFactory.Object);
            connection.ConnectionString = _fixture.ConnectionString;
            connection.Open();
            connection.BeginTransaction();
            Assert.Equal(true, connection.HasActiveExplicitTransaction());
            // no Rollback or Commit; during internal Rollback while closing a connection a mocked exception will be thrown

            // act
            connection.Close();

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
            conn1.Open();
            var session = conn1.SfSession;
            conn1.Close();
            Assert.True(session.IsEstablished());
            await Task.Delay(SessionTimeoutSeconds * 1000).ConfigureAwait(false); // wait until the session is expired
            var conn2 = new SnowflakeDbConnection(_fixture.ConnectionString);

            // act
            conn2.Open(); // it gets a session from the caching pool firstly closing session of conn1 in background

            Awaiter.WaitUntilConditionOrTimeout(() => !session.IsEstablished(), TimeSpan.FromMilliseconds(TimeForBackgroundSessionCloseMillis));

            // assert
            Assert.False(session.IsEstablished());

            // cleanup
            conn2.Close();
        }
    }
}
