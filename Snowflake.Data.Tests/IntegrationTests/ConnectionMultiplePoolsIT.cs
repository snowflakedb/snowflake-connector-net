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
    public class ConnectionMultiplePoolsIT : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;

        public ConnectionMultiplePoolsIT(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixture = fixture;
            ConnectionManagerTestsFacade.RegisterDedicatedContext(this, ConnectionPoolType.MultipleConnectionPool);
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        public void Dispose()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            ConnectionManagerTestsFacade.UnregisterDedicatedContext(this);
        }

        [SFFact]
        public async Task TestBasicConnectionPool()
        {
            var connectionString = _fixture.ConnectionString + "minPoolSize=0;maxPoolSize=1;poolingEnabled=true";
            var conn1 = new SnowflakeDbConnection(connectionString);
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);
            await conn1.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString, null).GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestReuseSessionInConnectionPool() // old name: TestConnectionPool
        {
            var connectionString = _fixture.ConnectionString + "minPoolSize=1;poolingEnabled=true";
            var conn1 = new SnowflakeDbConnection(connectionString);
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);
            await conn1.CloseAsync(CancellationToken.None);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString;
            await conn2.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());

            await conn2.CloseAsync(CancellationToken.None);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(connectionString).GetCurrentPoolSize());
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
        }

        [SFFact]
        public async Task TestReuseSessionInConnectionPoolReachingMaxConnections() // old name: TestConnectionPoolFull
        {
            var connectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connectionString;
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString;
            await conn2.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn2.State);

            Assert.Equal(2, pool.GetCurrentPoolSize());
            await conn1.CloseAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);
            Assert.Equal(2, pool.GetCurrentPoolSize());

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = connectionString;
            await conn3.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = connectionString;
            await conn4.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn4.State);

            await conn3.CloseAsync(CancellationToken.None);
            Assert.Equal(2, pool.GetCurrentPoolSize());
            await conn4.CloseAsync(CancellationToken.None);
            Assert.Equal(2, pool.GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
            Assert.Equal(ConnectionState.Closed, conn4.State);
        }

        [SFFact]
        public async Task TestWaitForTheIdleConnectionWhenExceedingMaxConnectionsLimit()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "application=TestWaitForMaxSize1;waitingForIdleSessionTimeout=1s;maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());
            var conn1 = await OpenConnectionAsync(connectionString);
            var conn2 = await OpenConnectionAsync(connectionString);
            var watch = new StopWatch();

            // act
            watch.Start();
            var thrown = Assert.Throws<SnowflakeDbException>(() => OpenConnection(connectionString));
            watch.Stop();

            // assert
            Assert.Contains("Unable to connect. Could not obtain a connection from the pool within a given timeout", thrown.Message);
            Assert.InRange(watch.ElapsedMilliseconds, 1000, 1500);
            Assert.Equal(pool.GetCurrentPoolSize(), 2);

            // cleanup
            await conn1.CloseAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);
        }

        [SFFact]
        public async Task TestWaitForTheIdleConnectionWhenExceedingMaxConnectionsLimitAsync()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "application=TestWaitForMaxSize2;waitingForIdleSessionTimeout=1s;maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());
            var conn1 = OpenConnection(connectionString);
            var conn2 = OpenConnection(connectionString);
            var watch = new StopWatch();

            // act
            watch.Start();
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => OpenConnectionAsync(connectionString));
            watch.Stop();

            // assert
            Assert.Contains("Unable to connect", thrown.Message);
            Assert.True(thrown.InnerException is AggregateException);
            var nestedException = ((AggregateException)thrown.InnerException).InnerException;
            Assert.Contains("Could not obtain a connection from the pool within a given timeout", nestedException.Message);
            Assert.InRange(watch.ElapsedMilliseconds, 1000, 1500);
            Assert.Equal(pool.GetCurrentPoolSize(), 2);

            // cleanup
            await conn1.CloseAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);
        }

        [SFFact]
        public async Task TestWaitInAQueueForAnIdleSession()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "application=TestWaitForMaxSize3;waitingForIdleSessionTimeout=3s;maxPoolSize=2;minPoolSize=0;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPoolInternal(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());
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
            Assert.Equivalent(new[] { "C", "D" }, waitingEvents.Select(e => e.ThreadName)); // equivalent = in any order
            var connectedEvents = events.Where(e => e.IsConnectedEvent()).ToList();
            Assert.Equal(4, connectedEvents.Count);
            var firstConnectedEventsGroup = connectedEvents.GetRange(0, 2);
            Assert.Equivalent(new[] { "A", "B" }, firstConnectedEventsGroup.Select(e => e.ThreadName));
            var lastConnectingEventsGroup = connectedEvents.GetRange(2, 2);
            Assert.Equivalent(new[] { "C", "D" }, lastConnectingEventsGroup.Select(e => e.ThreadName));
            Assert.True(firstConnectedEventsGroup[0].Duration <= ConnectPessimisticEstimate);
            Assert.True(firstConnectedEventsGroup[1].Duration <= ConnectPessimisticEstimate);
            // first to wait from C and D should first to connect, because we won't create a new session, we just reuse sessions returned by A and B threads
            Assert.Equal(waitingEvents[0].ThreadName, lastConnectingEventsGroup[0].ThreadName);
            Assert.Equal(waitingEvents[1].ThreadName, lastConnectingEventsGroup[1].ThreadName);
            Assert.InRange(lastConnectingEventsGroup[0].Duration, CMinConnectDuration - MeasurementTolerance, CMaxConnectDuration);
            Assert.InRange(lastConnectingEventsGroup[1].Duration, DMinConnectDuration - MeasurementTolerance, DMaxConnectDuration);
        }

        [SFFact]
        public async Task TestBusyAndIdleConnectionsCountedInPoolSize()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = connectionString;

            // act
            await connection.OpenAsync(CancellationToken.None);

            // assert
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // act
            await connection.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(1, pool.GetCurrentPoolSize());
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
        public async Task TestConnectionPoolDisable()
        {
            // arrange
            var pool = SnowflakeDbConnectionPool.GetPool(_fixture.ConnectionString + ";poolingEnabled=false");
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString;

            // act
            await conn1.OpenAsync(CancellationToken.None);

            // assert
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            await conn1.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestNewConnectionPoolClean()
        {
            var connectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=1;poolingEnabled=true;";
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connectionString;
            await conn1.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = connectionString + "retryCount=1";
            await conn2.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn2.State);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = connectionString + "retryCount=2";
            await conn3.OpenAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Open, conn3.State);

            await conn1.CloseAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(conn2.ConnectionString).GetCurrentPoolSize());
            SnowflakeDbConnectionPool.ClearAllPools();
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
            Assert.Equal(0, SnowflakeDbConnectionPool.GetPool(conn2.ConnectionString).GetCurrentPoolSize());
            await conn3.CloseAsync(CancellationToken.None);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetPool(conn3.ConnectionString).GetCurrentPoolSize());

            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(ConnectionState.Closed, conn2.State);
            Assert.Equal(ConnectionState.Closed, conn3.State);
        }

        [SFFact]
        public async Task TestConnectionPoolExpirationWorks()
        {
            // arrange
            const int ExpirationTimeoutInSeconds = 1;
            var connectionString = _fixture.ConnectionString + $"expirationTimeout={ExpirationTimeoutInSeconds};maxPoolSize=4;minPoolSize=2;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPoolInternal(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());

            // act
            var conn1 = OpenConnection(connectionString);
            var conn2 = OpenConnection(connectionString);
            var conn3 = OpenConnection(connectionString);
            var conn4 = OpenConnection(connectionString);

            // assert
            Assert.Equal(4, pool.GetCurrentPoolSize());

            // act
            await WaitUntilAllSessionsCreatedOrTimeout(pool);
            var beforeSleepMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await Task.Delay(TimeSpan.FromSeconds(ExpirationTimeoutInSeconds));
            await conn1.CloseAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);
            await conn3.CloseAsync(CancellationToken.None);
            await conn4.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(2, pool.GetCurrentPoolSize()); // 2 idle sessions, but expired because close doesn't remove expired sessions

            // act
            await WaitUntilAllSessionsCreatedOrTimeout(pool);
            var conn5 = await OpenConnectionAsync(connectionString);
            await WaitUntilAllSessionsCreatedOrTimeout(pool);

            // assert
            Assert.Equal(2, pool.GetCurrentPoolSize()); // 1 idle session and 1 busy
            var sessionStartTimes = pool.GetIdleSessionsStartTimes();
            Assert.Equal(1, sessionStartTimes.Count);
            Assert.True(sessionStartTimes.First() > beforeSleepMillis);
            Assert.True(conn5.SfSession.GetStartTime() > beforeSleepMillis);
        }

        [SFFact]
        public async Task TestMinPoolSize()
        {
            // arrange
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = _fixture.ConnectionString + "application=TestMinPoolSize;minPoolSize=3;poolingEnabled=true";

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
            var connection = OpenConnection(connectionString);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            await connection.CloseAsync(CancellationToken.None);

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestReleaseConnectionWhenRollbackFails()
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

        private async Task WaitUntilAllSessionsCreatedOrTimeout(SessionPool pool)
        {
            var expectingToWaitAtMostForSessionCreations = TimeSpan.FromSeconds(15);
            await Awaiter.WaitUntilConditionOrTimeout(() => pool.OngoingSessionCreationsCount() == 0, expectingToWaitAtMostForSessionCreations);
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
            await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            return connection;
        }

        [SFTheory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task TestReturningCancelledSessionsToThePool(bool cancelAsync)
        {
            var connectionString = _fixture.ConnectionString + "minPoolSize=0;maxPoolSize=2;application=TestReturningCancelledSessionsToThePool;poolingEnabled=true";

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

            await Awaiter.WaitUntilConditionOrTimeout(() =>
            {
                var state = pool.GetCurrentState();
                return state.IdleSessionsCount == 0 && state.BusySessionsCount == 1;
            }, TimeSpan.FromMilliseconds(1000));

            // one busy session
            Assert.Equal(0, pool.GetCurrentState().IdleSessionsCount);
            Assert.Equal(1, pool.GetCurrentState().BusySessionsCount);

            if (cancelAsync)
#if NET8_0_OR_GREATER
                await cts.CancelAsync();
#else
                cts.Cancel();
#endif
            else
                cts.Cancel();

            // operation cancelled properly
            var thrown = Assert.Throws<AggregateException>(() => task.Wait());
            Assert.IsAssignableFrom<OperationCanceledException>(thrown.InnerException);

            // one idle session
            Assert.Equal(1, pool.GetCurrentState().IdleSessionsCount);
            Assert.Equal(0, pool.GetCurrentState().BusySessionsCount);
        }
    }
}
