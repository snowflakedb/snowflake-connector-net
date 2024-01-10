using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
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
            var connectionString = ConnectionString + "application=TestWaitForMaxSize1";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "expecting pool to be empty");
            pool.SetMaxPoolSize(2);
            pool.SetWaitingForSessionToReuseTimeout(1000);
            var conn1 = OpenedConnection(connectionString);
            var conn2 = OpenedConnection(connectionString);
            var watch = new StopWatch();
            
            // act
            watch.Start();
            var start = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var thrown = Assert.Throws<SnowflakeDbException>(() => OpenedConnection(connectionString));
            var stop = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            watch.Stop();
            
            // assert
            Assert.That(thrown.Message, Does.Contain("Unable to connect. Could not obtain a connection from the pool within a given timeout"));
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(1000, 1500));
            Assert.AreEqual(pool.GetCurrentPoolSize(), 2);

            // cleanup
            conn1.Close();
            conn2.Close();
        }
        
        [Test]
        public void TestWaitForTheIdleConnectionWhenExceedingMaxConnectionsLimitAsync()
        {
            // arrange
            var connectionString = ConnectionString + "application=TestWaitForMaxSize2";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "expecting pool to be empty");
            pool.SetMaxPoolSize(2);
            pool.SetWaitingForSessionToReuseTimeout(1000);
            var conn1 = OpenedConnection(connectionString);
            var conn2 = OpenedConnection(connectionString);
            var watch = new StopWatch();
            
            // act
            watch.Start();
            var thrown = Assert.ThrowsAsync<SnowflakeDbException>(() => OpenedConnectionAsync(connectionString));
            watch.Stop();

            // assert
            Assert.That(thrown.Message, Does.Contain("Unable to connect"));
            Assert.IsTrue(thrown.InnerException is AggregateException);
            var nextedException = ((AggregateException)thrown.InnerException).InnerException;
            Assert.That(nextedException.Message, Does.Contain("Could not obtain a connection from the pool within a given timeout"));
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(1000, 1500));
            Assert.AreEqual(pool.GetCurrentPoolSize(), 2);

            // cleanup
            conn1.Close();
            conn2.Close();
        }

        [Test]
        public void TestWaitInAQueueForAnIdleSession()
        {
            // arrange
            var connectionString = ConnectionString + "application=TestWaitForMaxSize3";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "the pool is expected to be empty");
            pool.SetMaxPoolSize(2);
            pool.SetWaitingForSessionToReuseTimeout(3000);
            const long ADelay = 0;
            const long BDelay = 400;
            const long CDelay = 2 * BDelay;
            const long DDelay = 3 * BDelay;
            const long ABDelayAfterConnect = 2000;
            const long ConnectPessimisticEstimate = 1300;
            const long StartDelayPessimisticEstimate = 300;
            const long AMinConnectionReleaseTime = ADelay + ABDelayAfterConnect; // 2000
            const long AMaxConnectionReleaseTime = ADelay + StartDelayPessimisticEstimate + ConnectPessimisticEstimate + ABDelayAfterConnect; // 3600
            const long BMinConnectionReleaseTime = BDelay + ABDelayAfterConnect; // 2400
            const long BMaxConnectionReleaseTime = BDelay + StartDelayPessimisticEstimate + ConnectPessimisticEstimate + ABDelayAfterConnect; // 4000
            const long CMinConnectDuration = AMinConnectionReleaseTime - CDelay - StartDelayPessimisticEstimate; // 2000 - 800 - 300 = 900
            const long CMaxConnectDuration = AMaxConnectionReleaseTime - CDelay; // 3600 - 800 = 2800
            const long DMinConnectDuration = BMinConnectionReleaseTime - DDelay - StartDelayPessimisticEstimate; // 2400 - 1200 - 300 = 900
            const long DMaxConnectDuration = BMaxConnectionReleaseTime - DDelay; // 3600 - 800 = 2800
            
            var threads = new ConnectingThreads(connectionString)
                .NewThread("A", ADelay, ABDelayAfterConnect, true)
                .NewThread("B", BDelay, ABDelayAfterConnect, true)
                .NewThread("C", CDelay, 0, true)
                .NewThread("D", DDelay, 0, true);
            pool.SetSessionPoolEventHandler(new SessionPoolThreadEventHandler(threads));
            
            // act
            threads.StartAll().JoinAll();

            // assert
            var events = threads.Events().ToList();
            Assert.AreEqual(6, events.Count);
            var waitingEvents = events.Where(e => e.IsWaitingEvent()).ToList();
            Assert.AreEqual(2, waitingEvents.Count);
            CollectionAssert.AreEquivalent(new[] { "C", "D" }, waitingEvents.Select(e => e.ThreadName)); // equivalent = in any order
            var connectedEvents = events.Where(e => e.IsConnectedEvent()).ToList();
            Assert.AreEqual(4, connectedEvents.Count);
            var firstConnectedEventsGroup = connectedEvents.GetRange(0, 2);
            CollectionAssert.AreEquivalent(new[] { "A", "B" }, firstConnectedEventsGroup.Select(e => e.ThreadName));
            var lastConnectingEventsGroup = connectedEvents.GetRange(2, 2);
            CollectionAssert.AreEquivalent(new[] { "C", "D" }, lastConnectingEventsGroup.Select(e => e.ThreadName));
            Assert.LessOrEqual(firstConnectedEventsGroup[0].Duration, ConnectPessimisticEstimate);
            Assert.LessOrEqual(firstConnectedEventsGroup[1].Duration, ConnectPessimisticEstimate);
            // first to wait from C and D should first to connect, because we won't create a new session, we just reuse sessions returned by A and B threads
            Assert.AreEqual(waitingEvents[0].ThreadName, lastConnectingEventsGroup[0].ThreadName);
            Assert.AreEqual(waitingEvents[1].ThreadName, lastConnectingEventsGroup[1].ThreadName);
            Assert.That(lastConnectingEventsGroup[0].Duration, Is.InRange(CMinConnectDuration, CMaxConnectDuration)); 
            Assert.That(lastConnectingEventsGroup[1].Duration, Is.InRange(DMinConnectDuration, DMaxConnectDuration));
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

        private SnowflakeDbConnection OpenedConnection(string connectionString)
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;
            connection.Open();
            return connection;
        }
        
        private async Task<SnowflakeDbConnection> OpenedConnectionAsync(string connectionString)
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;
            await connection.OpenAsync().ConfigureAwait(false);
            return connection;
        }
    }
}
