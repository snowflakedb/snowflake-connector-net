using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [NonParallelizable]
    public class ConnectionMultiplePoolsIT : SFBaseTest
    {
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();

        private int WaitForPoolSize(SnowflakeDbSessionPool pool, int expectedSize, int maxRetries = 10)
        {
            return WaitForPoolSizeGeneric(() => pool.GetCurrentPoolSize(), expectedSize, maxRetries);
        }

        private int WaitForPoolSize(Snowflake.Data.Core.Session.SessionPool pool, int expectedSize, int maxRetries = 10)
        {
            return WaitForPoolSizeGeneric(() => pool.GetCurrentPoolSize(), expectedSize, maxRetries);
        }

        private int WaitForPoolSizeGeneric(Func<int> getCurrentSize, int expectedSize, int maxRetries = 10)
        {
            var retryCount = 0;
            while (getCurrentSize() != expectedSize && retryCount < maxRetries)
            {
                Thread.Sleep(50);
                retryCount++;
            }
            return getCurrentSize();
        }

        private void WaitForSessionState(Snowflake.Data.Core.Session.SessionPool pool, int expectedIdle, int expectedBusy, int maxRetries = 10)
        {
            var retryCount = 0;
            while (retryCount < maxRetries)
            {
                var state = pool.GetCurrentState();
                if (state.IdleSessionsCount == expectedIdle && state.BusySessionsCount == expectedBusy)
                    break;
                Thread.Sleep(50);
                retryCount++;
            }
        }

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

        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Test]
        public void TestBasicConnectionPool()
        {
            var connectionString = ConnectionString + "minPoolSize=0;maxPoolSize=1";
            var conn1 = new SnowflakeDbConnection(connectionString);
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            // assert - wait for key-pair auth to complete
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var actualSize = WaitForPoolSize(pool, 1, 50); // Increase retries for key-pair auth
            Assert.AreEqual(1, actualSize);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(connectionString, null).GetCurrentPoolSize());
        }

        [Test]
        public void TestReuseSessionInConnectionPool() // old name: TestConnectionPool
        {
            var connectionString = ConnectionString + "minPoolSize=1";
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
            var connectionString = ConnectionString + "maxPoolSize=2;minPoolSize=1";
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
        public void TestWaitForTheIdleConnectionWhenExceedingMaxConnectionsLimit()
        {
            // arrange
            var connectionString = ConnectionString + "application=TestWaitForMaxSize1;waitingForIdleSessionTimeout=1s;maxPoolSize=2;minPoolSize=1";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "expecting pool to be empty");
            var conn1 = OpenConnection(connectionString);
            var conn2 = OpenConnection(connectionString);
            var watch = new StopWatch();

            // act
            watch.Start();
            var thrown = Assert.Throws<SnowflakeDbException>(() => OpenConnection(connectionString));
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
            var connectionString = ConnectionString + "application=TestWaitForMaxSize2;waitingForIdleSessionTimeout=1s;maxPoolSize=2;minPoolSize=1";
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
        [Retry(3)]
        public void TestWaitInAQueueForAnIdleSession()
        {
            // arrange
            var connectionString = ConnectionString + "application=TestWaitForMaxSize3;waitingForIdleSessionTimeout=3s;maxPoolSize=2;minPoolSize=0";
            var pool = SnowflakeDbConnectionPool.GetPoolInternal(connectionString);
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "the pool is expected to be empty");
            const long ADelay = 0;
            const long BDelay = 400;
            const long CDelay = 2 * BDelay;
            const long DDelay = 3 * BDelay;
            const long ABDelayAfterConnect = 2000;
            const long ConnectPessimisticEstimate = 1300;
            const long StartDelayPessimisticEstimate = 350;
            const long AMinConnectionReleaseTime = ADelay + ABDelayAfterConnect; // 2000
            const long AMaxConnectionReleaseTime = ADelay + StartDelayPessimisticEstimate + ConnectPessimisticEstimate + ABDelayAfterConnect; // 3650
            const long BMinConnectionReleaseTime = BDelay + ABDelayAfterConnect; // 2400
            const long BMaxConnectionReleaseTime = BDelay + StartDelayPessimisticEstimate + ConnectPessimisticEstimate + ABDelayAfterConnect; // 4050
            const long CMinConnectDuration = AMinConnectionReleaseTime - CDelay - StartDelayPessimisticEstimate; // 2000 - 800 - 350 = 850
            const long CMaxConnectDuration = AMaxConnectionReleaseTime - CDelay; // 3650 - 800 = 2850
            const long DMinConnectDuration = BMinConnectionReleaseTime - DDelay - StartDelayPessimisticEstimate; // 2400 - 1200 - 350 = 850
            const long DMaxConnectDuration = BMaxConnectionReleaseTime - DDelay; // 3650 - 800 = 2850
            const long MeasurementTolerance = 25;

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
            Assert.AreEqual(6, events.Count); // A,B - connected; C,D - waiting, connected
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
            Assert.That(lastConnectingEventsGroup[0].Duration, Is.InRange(CMinConnectDuration - MeasurementTolerance, CMaxConnectDuration));
            Assert.That(lastConnectingEventsGroup[1].Duration, Is.InRange(DMinConnectDuration - MeasurementTolerance, DMaxConnectDuration));
        }

        [Test]
        public void TestBusyAndIdleConnectionsCountedInPoolSize()
        {
            // arrange
            var connectionString = ConnectionString + "maxPoolSize=2;minPoolSize=1";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;

            // act
            connection.Open();

            // assert - wait for key-pair auth to complete
            var actualSize = WaitForPoolSize(pool, 1, 50); // Increase retries for key-pair auth
            Assert.AreEqual(1, actualSize);

            // act
            connection.Close();

            // assert
            Assert.AreEqual(1, pool.GetCurrentPoolSize());
        }

        [Test]
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
            var connectionString = ConnectionString + "maxPoolSize=2;minPoolSize=1;";
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
            var pool1 = SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString);
            var pool2 = SnowflakeDbConnectionPool.GetPool(conn2.ConnectionString);
            var actualSize1 = WaitForPoolSize(pool1, 1, 50); // Increase retries for key-pair auth
            var actualSize2 = WaitForPoolSize(pool2, 1, 50); // Increase retries for key-pair auth
            Assert.AreEqual(1, actualSize1);
            Assert.AreEqual(1, actualSize2);
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
            var connectionString = ConnectionString + $"expirationTimeout={ExpirationTimeoutInSeconds};maxPoolSize=4;minPoolSize=2";
            var pool = SnowflakeDbConnectionPool.GetPoolInternal(connectionString);
            Assert.AreEqual(0, pool.GetCurrentPoolSize());

            // act
            var conn1 = OpenConnection(connectionString);
            var conn2 = OpenConnection(connectionString);
            var conn3 = OpenConnection(connectionString);
            var conn4 = OpenConnection(connectionString);

            // assert - wait for key-pair auth to complete
            var actualSize = WaitForPoolSize(pool, 4, 50); // Increase retries for key-pair auth
            Assert.AreEqual(4, actualSize);

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

        [Test]
        public void TestMinPoolSize()
        {
            // arrange
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = ConnectionString + "application=TestMinPoolSize;minPoolSize=3";

            // act
            connection.Open();

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            var actualSize = WaitForPoolSize(pool, 3, 50); // Increase retries for key-pair auth
            Assert.AreEqual(3, actualSize);

            // cleanup
            connection.Close();
        }

        [Test]
        public void TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connectionString = ConnectionString + "minPoolSize=0";
            var connection = OpenConnection(connectionString);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var actualSize = WaitForPoolSize(pool, 1, 50); // Increase retries for key-pair auth
            Assert.AreEqual(1, actualSize);

            // act
            connection.PreventPooling();
            connection.Close();

            // assert
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
        }

        [Test]
        public void TestReleaseConnectionWhenRollbackFails()
        {
            // arrange
            var connectionString = ConnectionString + "minPoolSize=0";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var commandThrowingExceptionOnlyForRollback = MockHelper.CommandThrowingExceptionOnlyForRollback();
            var mockDbProviderFactory = new Mock<DbProviderFactory>();
            mockDbProviderFactory.Setup(p => p.CreateCommand()).Returns(commandThrowingExceptionOnlyForRollback.Object);
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
            var connection = new TestSnowflakeDbConnection(mockDbProviderFactory.Object);
            connection.ConnectionString = connectionString;
            connection.Open();
            connection.BeginTransaction();
            Assert.AreEqual(true, connection.HasActiveExplicitTransaction());

            // act
            connection.Close();

            // assert
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "Should not return connection to the pool");
        }

        private void WaitUntilAllSessionsCreatedOrTimeout(SessionPool pool)
        {
            var expectingToWaitAtMostForSessionCreations = TimeSpan.FromSeconds(15);
            Awaiter.WaitUntilConditionOrTimeout(() => pool.OngoingSessionCreationsCount() == 0, expectingToWaitAtMostForSessionCreations);
        }

        private SnowflakeDbConnection OpenConnection(string connectionString)
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;
            connection.Open();
            return connection;
        }

        private async Task<SnowflakeDbConnection> OpenConnectionAsync(string connectionString)
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;
            await connection.OpenAsync().ConfigureAwait(false);
            return connection;
        }

        [Test]
        [Retry(3)]
        public void TestReturningCancelledSessionsToThePool([Values] bool cancelAsync)
        {
            var connectionString = ConnectionString + "minPoolSize=0;maxPoolSize=2;application=TestReturningCancelledSessionsToThePool";

            var pool = SnowflakeDbConnectionPool.ConnectionManager.GetPool(connectionString);
            pool.ClearSessions();

            // pool is empty - wait for clear to complete
            WaitForSessionState(pool, 0, 0);
            Assert.AreEqual(0, pool.GetCurrentState().IdleSessionsCount);
            Assert.AreEqual(0, pool.GetCurrentState().BusySessionsCount);

            var cts = new CancellationTokenSource();
            var task = Task.Run(async () =>
            {
                using (var connection = new SnowflakeDbConnection(connectionString))
                {
                    await connection.OpenAsync(cts.Token);
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"SELECT SYSTEM$WAIT(20)";
                        await command.ExecuteNonQueryAsync(cts.Token);
                    }
                }
            }, CancellationToken.None);

            Awaiter.WaitUntilConditionOrTimeout(() =>
            {
                var state = pool.GetCurrentState();
                return state.IdleSessionsCount == 0 && state.BusySessionsCount == 1;
            }, TimeSpan.FromMilliseconds(1000));

            // one busy session
            Assert.AreEqual(0, pool.GetCurrentState().IdleSessionsCount);
            Assert.AreEqual(1, pool.GetCurrentState().BusySessionsCount);

            if (cancelAsync)
#if NET8_0_OR_GREATER
                cts.CancelAsync();
#else
                cts.Cancel();
#endif
            else
                cts.Cancel();

            // operation cancelled properly
            var thrown = Assert.Throws<AggregateException>(() => task.Wait());
            Assert.IsInstanceOf<OperationCanceledException>(thrown.InnerException);

            // one idle session
            Assert.AreEqual(1, pool.GetCurrentState().IdleSessionsCount);
            Assert.AreEqual(0, pool.GetCurrentState().BusySessionsCount);
        }
    }
}
