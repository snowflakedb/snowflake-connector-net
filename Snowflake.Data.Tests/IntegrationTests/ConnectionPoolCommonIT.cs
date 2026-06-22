using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public abstract class ConnectionPoolCommonIT : SFBaseTestAsync, IDisposable
    {
        private readonly ConnectionPoolType _connectionPoolTypeUnderTest;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ConnectionPoolManager>();
        private readonly SFBaseTestAsyncFixture _fixture;

        internal ConnectionPoolCommonIT(SFBaseTestAsyncFixture fixture, ConnectionPoolType connectionPoolTypeUnderTest) : base(fixture)
        {
            _fixture = fixture;
            _connectionPoolTypeUnderTest = connectionPoolTypeUnderTest;
            ConnectionManagerTestsFacade.RegisterDedicatedContext(this, connectionPoolTypeUnderTest);
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(_connectionPoolTypeUnderTest);
            SnowflakeDbConnectionPool.ClearAllPools();
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                SnowflakeDbConnectionPool.SetPooling(true);
            }
            s_logger.Debug($"---------------- BeforeTest ---------------------");
            s_logger.Debug($"Testing Pool Type: {SnowflakeDbConnectionPool.GetConnectionPoolVersion()}");
        }

        public void Dispose()
        {
            ConnectionManagerTestsFacade.UnregisterDedicatedContext(this);
        }

        [SFFact]
        public void TestConnectionPoolMultiThreading()
        {
            Thread t1 = new Thread(() => _ = ThreadProcess1(_fixture.ConnectionString));
            Thread t2 = new Thread(() => _ = ThreadProcess2(_fixture.ConnectionString));

            t1.Start();
            t2.Start();

            t1.Join();
            t2.Join();
        }

        async Task ThreadProcess1(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            await conn1.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            await conn1.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            await Task.Delay(100).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Closed, conn1.State);
        }

        async Task ThreadProcess2(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            await conn1.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            await Task.Delay(100).ConfigureAwait(false);
            SFStatement statement = new SFStatement(conn1.SfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false, false);
            Assert.True(await resultSet.NextAsync().ConfigureAwait(false));
            Assert.Equal("1", resultSet.GetString(0));
            await conn1.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }

        [SFTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestConnectionPoolWithDispose(bool useOpenAsync)
        {
            Skip.When(useOpenAsync, "TODO Bug: SNOW-3552882");
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            }
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = "bad connection string";

            if (useOpenAsync)
            {
                await Assert.ThrowsAsync<SnowflakeDbException>(() => conn1.OpenAsync(CancellationToken.None)).ConfigureAwait(false);
            }
            else
            {
                Assert.Throws<SnowflakeDbException>(() => conn1.Open());
            }
            await conn1.CloseAsync(CancellationToken.None).ConfigureAwait(false);

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

        [SFFact]
        public void TestFailWhenPreventingFromReturningToPoolNotOpenedConnection()
        {
            // arrange
            var connection = new SnowflakeDbConnection(_fixture.ConnectionString);

            // act
            var thrown = Assert.Throws<Exception>(() => connection.PreventPooling());

            // assert
            Assert.Contains("Session not yet created for this connection. Unable to prevent the session from pooling", thrown.Message);
        }

        [SFFact]
        public async Task TestRollbackTransactionOnPooledWhenExceptionOccurred()
        {
            var connectionString = SetPoolWithOneElement();
            object firstOpenedSessionId;
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                firstOpenedSessionId = connection.SfSession.sessionId;
                await connection.BeginTransactionAsync().ConfigureAwait(false);
                Assert.True(connection.HasActiveExplicitTransaction());
                await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "invalid command will throw exception and leave session with an unfinished transaction";
                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                }).ConfigureAwait(false);
            }

            using (var connectionWithSessionReused = new SnowflakeDbConnection(connectionString))
            {
                await connectionWithSessionReused.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                Assert.Equal(firstOpenedSessionId, connectionWithSessionReused.SfSession.sessionId);
                Assert.False(connectionWithSessionReused.HasActiveExplicitTransaction());
                using (var cmd = connectionWithSessionReused.CreateCommand())
                {
                    cmd.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.Equal(DBNull.Value, await cmd.ExecuteScalarAsync().ConfigureAwait(false));
                }
            }

            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [SFFact]
        public async Task TestTransactionStatusNotTrackedForNonExplicitTransactionCalls()
        {
            var connectionString = SetPoolWithOneElement();
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "BEGIN"; // in general can be put as a part of a multi statement call and mixed with commit as well
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    Assert.False(connection.HasActiveExplicitTransaction());
                }
            }
        }

        [SFFact]
        public async Task TestRollbackTransactionOnPooledWhenConnectionClose()
        {
            var connectionString = SetPoolWithOneElement();
            Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            string firstOpenedSessionId;
            using (var connection1 = new SnowflakeDbConnection(connectionString))
            {
                await connection1.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(ExpectedPoolCountAfterOpen(), SnowflakeDbConnectionPool.GetCurrentPoolSize());
                await connection1.BeginTransactionAsync().ConfigureAwait(false);
                Assert.True(connection1.HasActiveExplicitTransaction());
                using (var command = connection1.CreateCommand())
                {
                    firstOpenedSessionId = connection1.SfSession.sessionId;
                    command.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.NotEqual(DBNull.Value, await command.ExecuteScalarAsync().ConfigureAwait(false));
                }
            }
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            using (var connection2 = new SnowflakeDbConnection(connectionString))
            {
                await connection2.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(ExpectedPoolCountAfterOpen(), SnowflakeDbConnectionPool.GetCurrentPoolSize());
                Assert.False(connection2.HasActiveExplicitTransaction());
                using (var command = connection2.CreateCommand())
                {
                    Assert.Equal(firstOpenedSessionId, connection2.SfSession.sessionId);
                    command.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.Equal(DBNull.Value, await command.ExecuteScalarAsync().ConfigureAwait(false));
                }
            }
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        private string SetPoolWithOneElement()
        {
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                SnowflakeDbConnectionPool.SetMaxPoolSize(1);
                return _fixture.ConnectionString;
            }
            return _fixture.ConnectionString + "maxPoolSize=1;minPoolSize=0;poolingEnabled=true";
        }

        private int ExpectedPoolCountAfterOpen()
        {
            return _connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache ? 0 : 1;
        }
    }

    public sealed class ConnectionPoolCommonSingleConnectionCacheIT : ConnectionPoolCommonIT
    {
        public ConnectionPoolCommonSingleConnectionCacheIT(SFBaseTestAsyncFixture fixture)
            : base(fixture, ConnectionPoolType.SingleConnectionCache)
        { }
    }

    public sealed class ConnctionPoolCommonMultipleConnectionIT : ConnectionPoolCommonIT
    {
        public ConnctionPoolCommonMultipleConnectionIT(SFBaseTestAsyncFixture fixture)
            : base(fixture, ConnectionPoolType.MultipleConnectionPool)
        { }
    }
}
