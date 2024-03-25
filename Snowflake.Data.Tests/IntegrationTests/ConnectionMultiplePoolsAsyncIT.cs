using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [NonParallelizable]
    public class ConnectionMultiplePoolsAsyncIT: SFBaseTestAsync
    {
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();
        
        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.SetConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
            SnowflakeDbConnectionPool.ClearAllPools();
        }
        
        [TearDown]
        public new void AfterTest()
        {
            _previousPoolConfig.Reset();
        }
        
        [Test]
        public async Task TestMinPoolSizeAsync()
        {
            // arrange
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = ConnectionString + "application=TestMinPoolSizeAsync;minPoolSize=3";
            
            // act
            await connection.OpenAsync().ConfigureAwait(false);
            Thread.Sleep(3000);            

            // assert
            var pool = SnowflakeDbConnectionPool.GetPool(connection.ConnectionString);
            Assert.AreEqual(3, pool.GetCurrentPoolSize());

            // cleanup
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
