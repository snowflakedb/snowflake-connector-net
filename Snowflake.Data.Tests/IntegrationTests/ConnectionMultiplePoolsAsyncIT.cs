using System.Data;
using System.Data.Common;
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
        
        [Test]
        public async Task TestPreventConnectionFromReturningToPool()
        {
            // arrange
            var connectionString = ConnectionString + "minPoolSize=0";
            var connection = new SnowflakeDbConnection(connectionString);
            await connection.OpenAsync().ConfigureAwait(false);
            var pool = SnowflakeDbConnectionPool.GetPool(connectionString);
            Assert.AreEqual(1, pool.GetCurrentPoolSize());
            
            // act
            connection.PreventPooling();
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            
            // assert
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
        }
        
        [Test]
        public async Task TestReleaseConnectionWhenRollbackFailsAsync()
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
            await connection.OpenAsync().ConfigureAwait(false);
            connection.BeginTransaction(); // not using async version because it is not available on .net framework
            Assert.AreEqual(true, connection.HasActiveExplicitTransaction());
            
            // act
            await connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            
            // assert
            Assert.AreEqual(0, pool.GetCurrentPoolSize(), "Should not return connection to the pool");
        }
    }
}
