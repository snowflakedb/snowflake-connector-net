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

    [NonParallelizable]
    public class ConnectionMultiplePoolsIT : SFBaseTest
    {
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();

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

        [SFFact]
        public void TestWaitForTheIdleConnectionWhenExceedingMaxConnectionsLimit()
        {
            // arrange
            var connectionString = ConnectionString + "application=TestWaitForMaxSize1;waitingForIdleSessionTimeout=1s;maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize(), "expecting pool to be empty");
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
            Assert.Equal(pool.GetCurrentPoolSize(), 2);

            // cleanup
            conn1.Close();
            conn2.Close();
        }

        [SFFact]
        [Retry(3)]
        public void TestWaitInAQueueForAnIdleSession()
        {
            // arrange
            var connectionString = ConnectionString + "application=TestWaitForMaxSize3;waitingForIdleSessionTimeout=3s;maxPoolSize=2;minPoolSize=0;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPoolInternal(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize(), "the pool is expected to be empty");
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
            Assert.Equal(6, events.Count); // A,B - connected; C,D - waiting, connected
            var waitingEvents = events.Where(e => e.IsWaitingEvent()).ToList();
            Assert.Equal(2, waitingEvents.Count);
            CollectionAssert.Equivalent(new[] { "C", "D" }, waitingEvents.Select(e => e.ThreadName)); // equivalent = in any order
            var connectedEvents = events.Where(e => e.IsConnectedEvent()).ToList();
            Assert.Equal(4, connectedEvents.Count);
            var firstConnectedEventsGroup = connectedEvents.GetRange(0, 2);
            CollectionAssert.Equivalent(new[] { "A", "B" }, firstConnectedEventsGroup.Select(e => e.ThreadName));
            var lastConnectingEventsGroup = connectedEvents.GetRange(2, 2);
            CollectionAssert.Equivalent(new[] { "C", "D" }, lastConnectingEventsGroup.Select(e => e.ThreadName));
            Assert.LessOrEqual(firstConnectedEventsGroup[0].Duration, ConnectPessimisticEstimate);
            Assert.LessOrEqual(firstConnectedEventsGroup[1].Duration, ConnectPessimisticEstimate);
            // first to wait from C and D should first to connect, because we won't create a new session, we just reuse sessions returned by A and B threads
            Assert.Equal(waitingEvents[0].ThreadName, lastConnectingEventsGroup[0].ThreadName);
            Assert.Equal(waitingEvents[1].ThreadName, lastConnectingEventsGroup[1].ThreadName);
            Assert.That(lastConnectingEventsGroup[0].Duration, Is.InRange(CMinConnectDuration - MeasurementTolerance, CMaxConnectDuration));
            Assert.That(lastConnectingEventsGroup[1].Duration, Is.InRange(DMinConnectDuration - MeasurementTolerance, DMaxConnectDuration));
        }

        [SFFact]
        public void TestConnectionPoolNotPossibleToDisableForAllPools()
        {
            // act
            var thrown = Assert.Throws<Exception>(() => SnowflakeDbConnectionPool.SetPooling(false));

            // assert
            Assert.NotNull(thrown);
        }

        [SFFact]
        public void TestMinPoolSize()
        {
            // arrange
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = ConnectionString + "application=TestMinPoolSize;minPoolSize=3;poolingEnabled=true";

            // act
            connection.Open();

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            Awaiter.WaitUntilConditionOrTimeout(() => pool.GetCurrentPoolSize() == 3, TimeSpan.FromMilliseconds(1000));
            Assert.Equal(3, pool.GetCurrentPoolSize());

            // cleanup
            connection.Close();
        }

        [SFFact]
        public void TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connectionString = ConnectionString + "minPoolSize=0;poolingEnabled=true";
            var connection = OpenConnection(connectionString);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            connection.Close();

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact]
        public void TestReleaseConnectionWhenRollbackFails()
        {
            // arrange
            var connectionString = ConnectionString + "minPoolSize=0;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var commandThrowingExceptionOnlyForRollback = MockHelper.CommandThrowingExceptionOnlyForRollback();
            var mockDbProviderFactory = new Mock<DbProviderFactory>();
            mockDbProviderFactory.Setup(p => p.CreateCommand()).Returns(commandThrowingExceptionOnlyForRollback.Object);
            Assert.Equal(0, pool.GetCurrentPoolSize());
            var connection = new TestSnowflakeDbConnection(mockDbProviderFactory.Object);
            connection.ConnectionString = connectionString;
            connection.Open();
            connection.BeginTransaction();
            Assert.Equal(true, connection.HasActiveExplicitTransaction());

            // act
            connection.Close();

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize(), "Should not return connection to the pool");
        }

        private SnowflakeDbConnection OpenConnection(string connectionString)
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;
            connection.Open();
            return connection;
        }

        [SFFact]
        [Retry(3)]
        public void TestReturningCancelledSessionsToThePool([Values] bool cancelAsync)
        {
            var connectionString = ConnectionString + "minPoolSize=0;maxPoolSize=2;application=TestReturningCancelledSessionsToThePool;poolingEnabled=true";

            var pool = SnowflakeDbConnectionPool.ConnectionManager.GetPool(connectionString);
            pool.ClearSessions();

            // pool is empty
            Assert.Equal(0, pool.GetCurrentState().IdleSessionsCount);
            Assert.Equal(0, pool.GetCurrentState().BusySessionsCount);

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
            Assert.Equal(0, pool.GetCurrentState().IdleSessionsCount);
            Assert.Equal(1, pool.GetCurrentState().BusySessionsCount);

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
            Assert.InstanceOf<OperationCanceledException>(thrown.InnerException);

            // one idle session
            Assert.Equal(1, pool.GetCurrentState().IdleSessionsCount);
            Assert.Equal(0, pool.GetCurrentState().BusySessionsCount);
        }
    }
}
