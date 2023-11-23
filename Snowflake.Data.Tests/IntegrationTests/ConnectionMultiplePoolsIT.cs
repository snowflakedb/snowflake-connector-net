using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [NonParallelizable]
    public class ConnectionMultiplePoolsIT: SFBaseTest
    {
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();
        
        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.SetConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetPooling(true);
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
        public void TestReuseSessionInConnectionPool() // old name: TestConnectionPool
        {
            var conn1 = new SnowflakeDbConnection(ConnectionString);
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString;
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());

            conn2.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
        }
        
        [Test]
        public void TestReuseSessionInConnectionPoolReachingMaxConnections() // old name: TestConnectionPoolFull
        {
            var pool = SnowflakeDbConnectionPool.GetPool(ConnectionString);
            pool.SetMaxPoolSize(2);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString;
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);
            
            Assert.AreEqual(2, pool.GetCurrentPoolSize());
            conn1.Close();
            conn2.Close();
            Assert.AreEqual(2, pool.GetCurrentPoolSize());
            
            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = ConnectionString;
            conn3.Open();
            Assert.AreEqual(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = ConnectionString;
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
        public void TestWaitForTheIdleConnectionWhenExceedingMaxConnectionsLimit()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(ConnectionString);
            pool.SetMaxPoolSize(2);
            pool.SetWaitingTimeout(1000);
            var conn1 = OpenedConnection();
            var conn2 = OpenedConnection();
            var watch = new Stopwatch();
            
            // act
            watch.Start();
            var thrown = Assert.Throws<SnowflakeDbException>(() => OpenedConnection());
            watch.Stop();

            // assert
            Assert.That(thrown.Message, Does.Contain("Unable to connect. Could not obtain a connection from the pool within a given timeout"));
            Assert.GreaterOrEqual(watch.ElapsedMilliseconds, 1000);
            Assert.LessOrEqual(watch.ElapsedMilliseconds, 1500);
            Assert.AreEqual(pool.GetCurrentPoolSize(), 2);

            // cleanup
            conn1.Close();
            conn2.Close();
        }

        [Test]
        public void TestWaitInAQueueForAnIdleSession()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(ConnectionString);
            pool.SetMaxPoolSize(2);
            pool.SetWaitingTimeout(3000);
            var threads = new ConnectingThreads(ConnectionString)
                .NewThread("A", 0, 2000, true)
                .NewThread("B", 50, 2000, true)
                .NewThread("C", 100, 0, true)
                .NewThread("D", 150, 0, true);
            var watch = new Stopwatch();
            
            // act
            watch.Start();
            threads.StartAll().JoinAll();
            watch.Stop();

            // assert
            var events = threads.Events().ToList();
            Assert.AreEqual(4, events.Count);
            CollectionAssert.AreEqual(
                new[]
                {
                    Tuple.Create("A", "CONNECTED"),
                    Tuple.Create("B", "CONNECTED"),
                    Tuple.Create("C", "CONNECTED"),
                    Tuple.Create("D", "CONNECTED")
                },
                events.Select(e => Tuple.Create(e.ThreadName, e.EventName)));
            Assert.LessOrEqual(events[0].Duration, 1000);
            Assert.LessOrEqual(events[1].Duration, 1000);
            Assert.GreaterOrEqual(events[2].Duration, 2000);
            Assert.LessOrEqual(events[2].Duration, 3100);
            Assert.GreaterOrEqual(events[3].Duration, 2000);
            Assert.LessOrEqual(events[3].Duration, 3100);
        }
        
        [Test]
        public void TestBusyAndIdleConnectionsCountedInPoolSize()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(ConnectionString);
            pool.SetMaxPoolSize(2);
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = ConnectionString;
            
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
        [Ignore("Enable when disabling pooling in connection string enabled - SNOW-902632")]
        public void TestConnectionPoolNotPossibleToDisableForAllPools()
        {
            // act
            var thrown = Assert.Throws<Exception>(() => SnowflakeDbConnectionPool.SetPooling(false));
            
            // assert
            Assert.IsNotNull(thrown);
        }
        
        [Test]
        public void TestConnectionPoolDisable() 
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(ConnectionString);
            pool.SetPooling(false);
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
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString + " retryCount=1";
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = ConnectionString + "  retryCount=2";
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

        private SnowflakeDbConnection OpenedConnection()
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = ConnectionString;
            connection.Open();
            return connection;
        }
    }
}
