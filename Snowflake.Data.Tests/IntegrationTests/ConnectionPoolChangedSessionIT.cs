using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [NonParallelizable]
    public class ConnectionPoolChangedSessionIT : SFBaseTest
    {
        private readonly QueryExecResponseData _queryExecResponseChangedRole = new()
        {
            finalDatabaseName = TestEnvironment.TestConfig.database,
            finalSchemaName = TestEnvironment.TestConfig.schema,
            finalRoleName = "role change",
            finalWarehouseName = TestEnvironment.TestConfig.warehouse
        };

        private readonly QueryExecResponseData _queryExecResponseChangedDatabase = new()
        {
            finalDatabaseName = "database changed",
            finalSchemaName = TestEnvironment.TestConfig.schema,
            finalRoleName = TestEnvironment.TestConfig.role,
            finalWarehouseName = TestEnvironment.TestConfig.warehouse
        };

        private readonly QueryExecResponseData _queryExecResponseChangedSchema = new()
        {
            finalDatabaseName = TestEnvironment.TestConfig.database,
            finalSchemaName = "schema changed",
            finalRoleName = TestEnvironment.TestConfig.role,
            finalWarehouseName = TestEnvironment.TestConfig.warehouse
        };

        private readonly QueryExecResponseData _queryExecResponseChangedWarehouse = new()
        {
            finalDatabaseName = TestEnvironment.TestConfig.database,
            finalSchemaName = TestEnvironment.TestConfig.schema,
            finalRoleName = TestEnvironment.TestConfig.role,
            finalWarehouseName = "warehouse changed"
        };

        private static PoolConfig s_previousPoolConfigRestorer;

        [OneTimeSetUp]
        public static void BeforeAllTests()
        {
            s_previousPoolConfigRestorer = new PoolConfig();
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
        }

        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [TearDown]
        public new void AfterTest()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            s_previousPoolConfigRestorer.Reset();
        }

        [Test]
        public void TestPoolDestroysConnectionWhenChangedSessionProperties()
        {
            var connectionString = ConnectionString + "application=Destroy;ChangedSession=Destroy;minPoolSize=0;maxPoolSize=3;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            connection.SfSession.UpdateSessionProperties(_queryExecResponseChangedDatabase);
            connection.Close();

            Assert.AreEqual(0, pool.GetCurrentPoolSize());
        }

        [Test]
        public void TestPoolingWhenSessionPropertiesUnchanged()
        {
            var connectionString = ConnectionString + "application=NoSessionChanges;ChangedSession=Destroy;minPoolSize=0;maxPoolSize=3;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            connection.Close();

            Assert.AreEqual(1, pool.GetCurrentPoolSize());
        }

        [Test]
        public void TestPoolingWhenConnectionPropertiesChangedForOriginalPoolMode()
        {
            var connectionString = ConnectionString + "application=OriginalPoolMode;ChangedSession=OriginalPool;minPoolSize=0;maxPoolSize=3;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            connection.SfSession.UpdateSessionProperties(_queryExecResponseChangedWarehouse);
            var sessionId = connection.SfSession.sessionId;
            connection.Close();

            Assert.AreEqual(1, pool.GetCurrentPoolSize());
            connection.Close();

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.AreEqual(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }

        [Test]
        public void TestPoolingWhenConnectionPropertiesChangedForDefaultPoolMode()
        {
            var connectionString = ConnectionString + "application=DefaultPoolMode;minPoolSize=0;maxPoolSize=3;poolingEnabled=true";
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            connection.SfSession.UpdateSessionProperties(_queryExecResponseChangedRole);
            var sessionId = connection.SfSession.sessionId;
            connection.Close();

            Assert.AreEqual(0, pool.GetCurrentPoolSize());

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.AreNotEqual(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }

        [Test]
        [Retry(3)]
        public void TestPoolDestroysAndRecreatesConnection()
        {
            var connectionString = ConnectionString + "application=DestroyRecreateSession;ChangedSession=Destroy;minPoolSize=1;maxPoolSize=3;poolingEnabled=true";

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            var sessionId = connection.SfSession.sessionId;
            connection.SfSession.UpdateSessionProperties(_queryExecResponseChangedSchema);
            connection.Close();

            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(1, pool.GetCurrentPoolSize());

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.AreNotEqual(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }

        [Test]
        public void TestCompareSessionChangesCaseInsensitiveWhenUnquoted()
        {
            var connectionString = ConnectionString + "application=CompareCaseInsensitive;ChangedSession=Destroy;minPoolSize=1;maxPoolSize=3;poolingEnabled=true";

            var responseData = new QueryExecResponseData()
            {
                finalDatabaseName = TestEnvironment.TestConfig.database.ToLower(),
                finalSchemaName = TestEnvironment.TestConfig.schema.ToUpper(),
                finalRoleName = $"{char.ToUpper(TestEnvironment.TestConfig.role[0])}{TestEnvironment.TestConfig.role.Substring(1).ToLower()}",
                finalWarehouseName = TestEnvironment.TestConfig.warehouse.ToLower()
            };

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            var sessionId = connection.SfSession.sessionId;
            connection.SfSession.UpdateSessionProperties(responseData);
            connection.Close();

            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(1, pool.GetCurrentPoolSize());

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.AreEqual(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }

        [Test]
        public void TestCompareSessionChangesCaseSensitiveWhenQuoted()
        {
            var connectionString = ConnectionString + "application=CompareCaseSensitive;ChangedSession=Destroy;minPoolSize=1;maxPoolSize=3;poolingEnabled=true";

            var responseData = new QueryExecResponseData()
            {
                finalDatabaseName = TestEnvironment.TestConfig.database,
                finalSchemaName = TestEnvironment.TestConfig.schema,
                finalRoleName = $"\\\"SomeQuotedValue\\\"",
                finalWarehouseName = TestEnvironment.TestConfig.warehouse.ToLower()
            };

            var connection = new SnowflakeDbConnection(connectionString);
            connection.Open();
            var sessionId = connection.SfSession.sessionId;
            connection.SfSession.UpdateSessionProperties(responseData);
            connection.Close();

            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(1, pool.GetCurrentPoolSize());

            var connection2 = new SnowflakeDbConnection(connectionString);
            connection2.Open();
            Assert.AreNotEqual(sessionId, connection2.SfSession.sessionId);
            connection2.Close();
        }
    }
}
