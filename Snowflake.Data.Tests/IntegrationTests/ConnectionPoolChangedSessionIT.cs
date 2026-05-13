using System;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class ConnectionPoolChangedSessionITFixture : IDisposable
    {
        private readonly PoolConfig _previousPoolConfigRestorer;

        public ConnectionPoolChangedSessionITFixture()
        {
            _previousPoolConfigRestorer = new PoolConfig();
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
        }

        public void Dispose()
        {
            _previousPoolConfigRestorer.Reset();
        }
    }

    public class ConnectionPoolChangedSessionIT : SFBaseTestAsync, IClassFixture<ConnectionPoolChangedSessionITFixture>, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public ConnectionPoolChangedSessionIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture, ConnectionPoolChangedSessionITFixture classFixture) : base(fixture, envFixture)
        {
            _fixture = fixture;
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        public void Dispose()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        private QueryExecResponseData _queryExecResponseChangedRole() => new()
        {
            finalDatabaseName = _fixture.testConfig.database,
            finalSchemaName = _fixture.testConfig.schema,
            finalRoleName = "role change",
            finalWarehouseName = _fixture.testConfig.warehouse
        };

        private QueryExecResponseData _queryExecResponseChangedDatabase() => new()
        {
            finalDatabaseName = "database changed",
            finalSchemaName = _fixture.testConfig.schema,
            finalRoleName = _fixture.testConfig.role,
            finalWarehouseName = _fixture.testConfig.warehouse
        };

        private QueryExecResponseData _queryExecResponseChangedSchema => new()
        {
            finalDatabaseName = _fixture.testConfig.database,
            finalSchemaName = "schema changed",
            finalRoleName = _fixture.testConfig.role,
            finalWarehouseName = _fixture.testConfig.warehouse
        };

        private QueryExecResponseData _queryExecResponseChangedWarehouse() => new()
        {
            finalDatabaseName = _fixture.testConfig.database,
            finalSchemaName = _fixture.testConfig.schema,
            finalRoleName = _fixture.testConfig.role,
            finalWarehouseName = "warehouse changed"
        };

        [Fact]
        public void TestPoolDestroysConnectionWhenChangedSessionProperties()
        {
            var connectionString = _fixture.ConnectionString + "application=Destroy;ChangedSession=Destroy;minPoolSize=0;maxPoolSize=3;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            connection.SfSession.UpdateSessionProperties(_queryExecResponseChangedDatabase());
            connection.Close();

            Assert.Equal(0, pool.GetCurrentPoolSize());
        }

        [Fact]
        public void TestPoolingWhenSessionPropertiesUnchanged()
        {
            var connectionString = _fixture.ConnectionString + "application=NoSessionChanges;ChangedSession=Destroy;minPoolSize=0;maxPoolSize=3;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            connection.Close();

            Assert.Equal(1, pool.GetCurrentPoolSize());
        }

        [Fact]
        public void TestPoolingWhenConnectionPropertiesChangedForOriginalPoolMode()
        {
            var connectionString = _fixture.ConnectionString + "application=OriginalPoolMode;ChangedSession=OriginalPool;minPoolSize=0;maxPoolSize=3;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            connection.SfSession.UpdateSessionProperties(_queryExecResponseChangedWarehouse());
            var sessionId = connection.SfSession.sessionId;
            connection.Close();

            Assert.Equal(1, pool.GetCurrentPoolSize());
            connection.Close();

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.Equal(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }

        [Fact]
        public void TestPoolingWhenConnectionPropertiesChangedForDefaultPoolMode()
        {
            var connectionString = _fixture.ConnectionString + "application=DefaultPoolMode;minPoolSize=0;maxPoolSize=3;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            connection.SfSession.UpdateSessionProperties(_queryExecResponseChangedRole());
            var sessionId = connection.SfSession.sessionId;
            connection.Close();

            Assert.Equal(0, pool.GetCurrentPoolSize());

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.NotEqual(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }

        [Fact]
        public void TestPoolDestroysAndRecreatesConnection()
        {
            var connectionString = _fixture.ConnectionString + "application=DestroyRecreateSession;ChangedSession=Destroy;minPoolSize=1;maxPoolSize=3;poolingEnabled=true";

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            var sessionId = connection.SfSession.sessionId;
            connection.SfSession.UpdateSessionProperties(_queryExecResponseChangedSchema);
            connection.Close();

            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.NotEqual(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }

        [Fact]
        public void TestCompareSessionChangesCaseInsensitiveWhenUnquoted()
        {
            var connectionString = _fixture.ConnectionString + "application=CompareCaseInsensitive;ChangedSession=Destroy;minPoolSize=1;maxPoolSize=3;poolingEnabled=true";

            var responseData = new QueryExecResponseData()
            {
                finalDatabaseName = _fixture.testConfig.database.ToLower(),
                finalSchemaName = _fixture.testConfig.schema.ToUpper(),
                finalRoleName = $"{char.ToUpper(_fixture.testConfig.role[0])}{_fixture.testConfig.role.Substring(1).ToLower()}",
                finalWarehouseName = _fixture.testConfig.warehouse.ToLower()
            };

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            var sessionId = connection.SfSession.sessionId;
            connection.SfSession.UpdateSessionProperties(responseData);
            connection.Close();

            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.Equal(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }

        [Fact]
        public void TestCompareSessionChangesCaseSensitiveWhenQuoted()
        {
            var connectionString = _fixture.ConnectionString + "application=CompareCaseSensitive;ChangedSession=Destroy;minPoolSize=1;maxPoolSize=3;poolingEnabled=true";

            var responseData = new QueryExecResponseData()
            {
                finalDatabaseName = _fixture.testConfig.database,
                finalSchemaName = _fixture.testConfig.schema,
                finalRoleName = $"\\\"SomeQuotedValue\\\"",
                finalWarehouseName = _fixture.testConfig.warehouse.ToLower()
            };

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            var sessionId = connection.SfSession.sessionId;
            connection.SfSession.UpdateSessionProperties(responseData);
            connection.Close();

            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.Equal(1, pool.GetCurrentPoolSize());

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.NotEqual(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }
    }
}
