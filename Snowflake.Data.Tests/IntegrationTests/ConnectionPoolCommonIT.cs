using System;
using System.Data;
using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture(ConnectionPoolType.SingleConnectionCache)]
    [TestFixture(ConnectionPoolType.MultipleConnectionPool)]
    [NonParallelizable]
    class ConnectionPoolCommonIT : SFBaseTest
    {
        private readonly ConnectionPoolType _connectionPoolTypeUnderTest;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ConnectionPoolManager>();
        private readonly PoolConfig _previousPoolConfig;

        public ConnectionPoolCommonIT(ConnectionPoolType connectionPoolTypeUnderTest)
        {
            _connectionPoolTypeUnderTest = connectionPoolTypeUnderTest;
            _previousPoolConfig = new PoolConfig();
        }

        [SetUp]
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

        [TearDown]
        public new void AfterTest()
        {
            _previousPoolConfig.Reset();
        }

        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Test]
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
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
        }

        void ThreadProcess2(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            conn1.Open();

            Thread.Sleep(1000);
            SFStatement statement = new SFStatement(conn1.SfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
            conn1.Close();
        }

        [Test]
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

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                Assert.AreEqual(0, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
            }
            else
            {
                var thrown = Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString));
                Assert.That(thrown.Message, Does.Contain("Connection string is invalid"));
            }
        }

        [Test]
        public void TestFailWhenPreventingFromReturningToPoolNotOpenedConnection()
        {
            // arrange
            var connection = new SnowflakeDbConnection(ConnectionString);

            // act
            var thrown = Assert.Throws<Exception>(() => connection.PreventPooling());

            // assert
            Assert.That(thrown.Message, Does.Contain("Session not yet created for this connection. Unable to prevent the session from pooling"));
        }

        [Test]
        public void TestRollbackTransactionOnPooledWhenExceptionOccurred()
        {
            var connectionString = SetPoolWithOneElement();
            object firstOpenedSessionId;
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                connection.Open();
                firstOpenedSessionId = connection.SfSession.sessionId;
                connection.BeginTransaction();
                Assert.AreEqual(true, connection.HasActiveExplicitTransaction());
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

                Assert.AreEqual(firstOpenedSessionId, connectionWithSessionReused.SfSession.sessionId);
                Assert.AreEqual(false, connectionWithSessionReused.HasActiveExplicitTransaction());
                using (var cmd = connectionWithSessionReused.CreateCommand())
                {
                    cmd.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.AreEqual(DBNull.Value, cmd.ExecuteScalar());
                }
            }

            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be reused and any pending transaction rolled back before it gets back to the pool");
        }

        [Test]
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
                    Assert.AreEqual(false, connection.HasActiveExplicitTransaction());
                }
            }
        }

        [Test]
        [Retry(3)]
        public void TestRollbackTransactionOnPooledWhenConnectionClose()
        {
            var connectionString = SetPoolWithOneElement();
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be returned to the pool");

            string firstOpenedSessionId;
            using (var connection1 = new SnowflakeDbConnection(connectionString))
            {
                connection1.Open();
                Assert.AreEqual(ExpectedPoolCountAfterOpen(), SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection session is added to the pool after close connection");
                connection1.BeginTransaction();
                Assert.AreEqual(true, connection1.HasActiveExplicitTransaction());
                using (var command = connection1.CreateCommand())
                {
                    firstOpenedSessionId = connection1.SfSession.sessionId;
                    command.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.AreNotEqual(DBNull.Value, command.ExecuteScalar());
                }
            }
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be returned to the pool");

            using (var connection2 = new SnowflakeDbConnection(connectionString))
            {
                connection2.Open();
                Assert.AreEqual(ExpectedPoolCountAfterOpen(), SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection session should be now removed from the pool");
                Assert.AreEqual(false, connection2.HasActiveExplicitTransaction());
                using (var command = connection2.CreateCommand())
                {
                    Assert.AreEqual(firstOpenedSessionId, connection2.SfSession.sessionId);
                    command.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.AreEqual(DBNull.Value, command.ExecuteScalar());
                }
            }
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be returned to the pool");
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
