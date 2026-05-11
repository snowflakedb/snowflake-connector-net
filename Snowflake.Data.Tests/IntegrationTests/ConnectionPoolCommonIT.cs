using System;
using System.Data;
using System.Threading;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    class ConnectionPoolCommonIT : SFBaseTest
    {
        private readonly ConnectionPoolType _connectionPoolTypeUnderTest;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ConnectionPoolManager>();
        private readonly PoolConfig _previousPoolConfig;

        public ConnectionPoolCommonIT(TestEnvironmentFixture envFixture, ConnectionPoolType connectionPoolTypeUnderTest) : base(envFixture)
        {
            _connectionPoolTypeUnderTest = connectionPoolTypeUnderTest;
            _previousPoolConfig = new PoolConfig();
        }
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(_connectionPoolTypeUnderTest);
            SnowflakeDbConnectionPool.ClearAllPools();
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                SnowflakeDbConnectionPool.SetPooling(true);
            }
            s_logger.Debug($"---------------- BeforeTest ---------------------");
            s_logger.Debug($"Testing Pool Type: {SnowflakeDbConnectionPool.GetConnectionPoolVersion()}");
        }
        public new void AfterTest()
        {
            _previousPoolConfig.Reset();
        }
        public static void AfterAllTests()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Fact]
        public void TestConnectionPoolMultiThreading()
        {
            Thread t1 = new Thread(() => ThreadProcess1(ConnectionString));
            Thread t2 = new Thread(() => ThreadProcess2(ConnectionString));

            t1.Start();
            t2.Start();

            t1.Join();
            t2.Join();
        }

        void ThreadProcess1(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            conn1.Open();
            conn1.Close();
            Thread.Sleep(1000);
            Assert.Equal(ConnectionState.Closed, conn1.State);
        }

        void ThreadProcess2(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            conn1.Open();

            Thread.Sleep(1000);
            SFStatement statement = new SFStatement(conn1.SfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false, false);
            Assert.Equal(true, resultSet.Next());
            Assert.Equal("1", resultSet.GetString(0));
            conn1.Close();
        }

        [Fact]
        public void TestConnectionPoolWithDispose()
        {
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            }
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = "bad connection string";
            Assert.Throws<SnowflakeDbException>(() => conn1.Open());
            conn1.Close();

            Assert.Equal(ConnectionState.Closed, conn1.State);
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
            }
            else
            {
                var thrown = Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString));
                Assert.Contains("Connection string is invalid", thrown.Message);
            }
        }

        [Fact]
        public void TestFailWhenPreventingFromReturningToPoolNotOpenedConnection()
        {
            // arrange
            var connection = new SnowflakeDbConnection(ConnectionString);

            // act
            var thrown = Assert.Throws<Exception>(() => connection.PreventPooling());

            // assert
            Assert.Contains("Session not yet created for this connection. Unable to prevent the session from pooling", thrown.Message);
        }

        [Fact]
        public void TestRollbackTransactionOnPooledWhenExceptionOccurred()
        {
            var connectionString = SetPoolWithOneElement();
            object firstOpenedSessionId;
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                connection.Open();
                firstOpenedSessionId = connection.SfSession.sessionId;
                connection.BeginTransaction();
                Assert.Equal(true, connection.HasActiveExplicitTransaction());
                Assert.Throws<SnowflakeDbException>(() =>
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "invalid command will throw exception and leave session with an unfinished transaction";
                        command.ExecuteNonQuery();
                    }
                });
            }

            using (var connectionWithSessionReused = new SnowflakeDbConnection(connectionString))
            {
                connectionWithSessionReused.Open();

                Assert.Equal(firstOpenedSessionId, connectionWithSessionReused.SfSession.sessionId);
                Assert.Equal(false, connectionWithSessionReused.HasActiveExplicitTransaction());
                using (var cmd = connectionWithSessionReused.CreateCommand())
                {
                    cmd.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.Equal(DBNull.Value, cmd.ExecuteScalar());
                }
            }

            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Fact]
        public void TestTransactionStatusNotTrackedForNonExplicitTransactionCalls()
        {
            var connectionString = SetPoolWithOneElement();
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "BEGIN"; // in general can be put as a part of a multi statement call and mixed with commit as well
                    command.ExecuteNonQuery();
                    Assert.Equal(false, connection.HasActiveExplicitTransaction());
                }
            }
        }

        [Fact]
        public void TestRollbackTransactionOnPooledWhenConnectionClose()
        {
            var connectionString = SetPoolWithOneElement();
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            string firstOpenedSessionId;
            using (var connection1 = new SnowflakeDbConnection(connectionString))
            {
                connection1.Open();
                Assert.Equal(ExpectedPoolCountAfterOpen(), SnowflakeDbConnectionPool.GetCurrentPoolSize());
                connection1.BeginTransaction();
                Assert.Equal(true, connection1.HasActiveExplicitTransaction());
                using (var command = connection1.CreateCommand())
                {
                    firstOpenedSessionId = connection1.SfSession.sessionId;
                    command.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.NotEqual(DBNull.Value, command.ExecuteScalar());
                }
            }
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            using (var connection2 = new SnowflakeDbConnection(connectionString))
            {
                connection2.Open();
                Assert.Equal(ExpectedPoolCountAfterOpen(), SnowflakeDbConnectionPool.GetCurrentPoolSize());
                Assert.Equal(false, connection2.HasActiveExplicitTransaction());
                using (var command = connection2.CreateCommand())
                {
                    Assert.Equal(firstOpenedSessionId, connection2.SfSession.sessionId);
                    command.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.Equal(DBNull.Value, command.ExecuteScalar());
                }
            }
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        private string SetPoolWithOneElement()
        {
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                SnowflakeDbConnectionPool.SetMaxPoolSize(1);
                return ConnectionString;
            }
            return ConnectionString + "maxPoolSize=1;minPoolSize=0;poolingEnabled=true";
        }

        private int ExpectedPoolCountAfterOpen()
        {
            return _connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache ? 0 : 1;
        }
    }
}
