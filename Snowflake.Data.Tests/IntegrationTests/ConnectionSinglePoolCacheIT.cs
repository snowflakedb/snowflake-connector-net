using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [NonParallelizable]
    public class ConnectionSinglePoolCacheIT: SFBaseTest
    {
        private static readonly PoolConfig s_previousPoolConfig = new PoolConfig();

        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.SetConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetPooling(true);
        }
        
        [TearDown]
        public new void AfterTest()
        {
            s_previousPoolConfig.Reset();
        }
        
        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }        
        
        [Test]
        public void TestConnectionPoolFull()
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
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            conn1.Close();
            conn2.Close();
            Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = ConnectionString + "  retryCount=2";
            conn3.Open();
            Assert.AreEqual(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = ConnectionString + "  retryCount=3";
            conn4.Open();
            Assert.AreEqual(ConnectionState.Open, conn4.State);

            conn3.Close();
            Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            conn4.Close();
            Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            Assert.AreEqual(ConnectionState.Closed, conn3.State);
            Assert.AreEqual(ConnectionState.Closed, conn4.State);
            SnowflakeDbConnectionPool.ClearAllPools();
        }
        
        [Test]
        public void TestConnectionPoolClean()
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
            Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            SnowflakeDbConnectionPool.ClearAllPools();
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            conn3.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            Assert.AreEqual(ConnectionState.Closed, conn3.State);
        }
    }
}
