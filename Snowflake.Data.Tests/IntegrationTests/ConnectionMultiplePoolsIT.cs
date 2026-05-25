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
            Assert.InRange(watch.ElapsedMilliseconds, 1000, 5000);
            Assert.Equal(pool.GetCurrentPoolSize(), 2);

            // cleanup
            await conn1.CloseAsync(CancellationToken.None);
            await conn2.CloseAsync(CancellationToken.None);
        }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        public void TestWaitInAQueueForAnIdleSession()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "application=TestWaitForMaxSize3;waitingForIdleSessionTimeout=15s;maxPoolSize=2;minPoolSize=0;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPoolInternal(connectionString);
            Assert.Equal(0, pool.GetCurrentPoolSize());
            const long ADelay = 0;
            const long BDelay = 400;
            const long CDelay = 2 * BDelay;
            const long DDelay = 3 * BDelay;
            const long ABDelayAfterConnect = 2000;
            const long ConnectPessimisticEstimate = 2500;
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
            Assert.InRange(firstConnectedEventsGroup[0].Duration, long.MinValue, ConnectPessimisticEstimate);
            Assert.InRange(firstConnectedEventsGroup[1].Duration, long.MinValue, ConnectPessimisticEstimate);
            // first to wait from C and D should first to connect, because we won't create a new session, we just reuse sessions returned by A and B threads
            Assert.Equal(waitingEvents[0].ThreadName, lastConnectingEventsGroup[0].ThreadName);
            Assert.Equal(waitingEvents[1].ThreadName, lastConnectingEventsGroup[1].ThreadName);
            Assert.InRange(lastConnectingEventsGroup[0].Duration, CMinConnectDuration - MeasurementTolerance, CMaxConnectDuration);
            Assert.InRange(lastConnectingEventsGroup[1].Duration, DMinConnectDuration - MeasurementTolerance, DMaxConnectDuration);
        }

        [SFFact]
        public void TestConnectionPoolNotPossibleToDisableForAllPools()
        {
            // act
            var thrown = Assert.Throws<Exception>(() => SnowflakeDbConnectionPool.SetPooling(false));

            // assert
            Assert.NotNull(thrown);
        }

        [SFFact(RetriesCount = RetriesCount.Once)]
        public async Task TestMinPoolSize()
        {
            // arrange
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = _fixture.ConnectionString + "application=TestMinPoolSize;minPoolSize=3;poolingEnabled=true";

            // act
            connection.Open();

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            await Awaiter.WaitUntilConditionOrTimeout(() => pool.GetCurrentPoolSize() == 3, TimeSpan.FromMilliseconds(1000)).ConfigureAwait(false);
            Assert.Equal(3, pool.GetCurrentPoolSize());

            // cleanup
            await connection.CloseAsync(CancellationToken.None);
        }

        [SFFact]
        public async Task TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connectionString = _fixture.ConnectionString + "minPoolSize=0;poolingEnabled=true";
            var connection = await OpenConnectionAsync(connectionString);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            // act
            connection.PreventPooling();
            connection.Close();

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
            connection.Close();

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [SFTheory(RetriesCount = RetriesCount.Once)]
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
            }, TimeSpan.FromMilliseconds(10000));

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

            // operation canceled properly
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);

            // one idle session
            Assert.Equal(1, pool.GetCurrentState().IdleSessionsCount);
            Assert.Equal(0, pool.GetCurrentState().BusySessionsCount);
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
    }
}
