using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [NonParallelizable]
    public class ConnectionSinglePoolCacheAsyncIT : SFBaseTestAsync
    {
        private readonly PoolConfig _previousPoolConfig = new PoolConfig();

        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [TearDown]
        public new void AfterTest()
        {
            _previousPoolConfig.Reset();
        }


        [Test]
        public async Task TestPutConnectionToPoolOnCloseAsync()
        {
            // arrange
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                Assert.AreEqual(conn.State, ConnectionState.Closed);
                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                await conn.OpenAsync(connectionCancelToken.Token).ConfigureAwait(false);

                // act
                await conn.CloseAsync(connectionCancelToken.Token).ConfigureAwait(false);

                // assert
                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            }
        }

        [Test]
        public async Task TestDoNotPutInvalidConnectionToPoolAsync()
        {
            // arrange
            var invalidConnectionString = ";connection_timeout=0";
            using (var conn = new SnowflakeDbConnection(invalidConnectionString))
            {
                Assert.AreEqual(conn.State, ConnectionState.Closed);
                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                try
                {
                    await conn.OpenAsync(connectionCancelToken.Token).ConfigureAwait(false);
                    Assert.Fail("OpenAsync should throw exception");
                }
                catch { }

                // act
                await conn.CloseAsync(connectionCancelToken.Token).ConfigureAwait(false);

                // assert
                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            }
        }

        [Test]
        public void TestConnectionPoolWithInvalidOpenAsync()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            // make the connection string unique so it won't pick up connection
            // pooled by other test cases.
            string connStr = ConnectionString + "application=conn_pool_test_invalid_openasync";
            using (var connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = connStr;
                // call openAsync but do not wait and destroy it direct
                // so the session is initialized with empty token
                connection.OpenAsync();
            }

            // use the same connection string to make a new connection
            // to ensure the invalid connection made previously is not pooled
            using (var connection1 = new SnowflakeDbConnection())
            {
                connection1.ConnectionString = connStr;
                // this will not open a new session but get the invalid connection from pool
                connection1.Open();
                // Now run query with connection1
                var command = connection1.CreateCommand();
                command.CommandText = "select 1, 2, 3";

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                // Process each column as appropriate
                                reader.GetFieldValue<object>(i);
                            }
                        }
                    }
                }
                catch (SnowflakeDbException)
                {
                    // fail the test case if anything wrong.
                    Assert.Fail();
                }
            }
        }

        [Test(Description = "test connection pooling with concurrent connection using async calls")]
        public void TestConcurrentConnectionPoolingAsync()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = ConnectionString + ";application=TestConcurrentConnectionPoolingAsync";
            SnowflakeDbConnectionPool.SetMaxPoolSize(3);
            SnowflakeDbConnectionPool.SetTimeout(1); // set short pooling timeout to cover the case that connection expired
            ConcurrentPoolingAsyncHelper(connStr, true, 5, 5, 3);
            SnowflakeDbConnectionPool.SetTimeout(3600);
        }

        [Test(Description = "test connection pooling with concurrent connection and using async calls no close call for connection. Connection is closed when Dispose() is called by framework.")]
        public void TestConcurrentConnectionPoolingDisposeAsync()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = ConnectionString + ";application=TestConcurrentConnectionPoolingDisposeAsync";
            SnowflakeDbConnectionPool.SetMaxPoolSize(3);
            SnowflakeDbConnectionPool.SetTimeout(1); // set short pooling timeout to cover the case that connection expired
            ConcurrentPoolingAsyncHelper(connStr, false, 5, 5, 3);
            SnowflakeDbConnectionPool.SetTimeout(3600);
        }

        public static void ConcurrentPoolingAsyncHelper(string connectionString, bool closeConnection, int tasksCount, int connectionsInTask, int abandonedConnectionsCount)
        {
            var tasks = new Task[tasksCount + 1];
            for (int i = 0; i < tasksCount; i++)
            {
                tasks[i] = QueryExecutionTaskAsync(connectionString, closeConnection, connectionsInTask);
            }
            // cover the case of invalid sessions to ensure that won't
            // break connection pooling
            tasks[tasksCount] = InvalidConnectionTaskAsync(connectionString, abandonedConnectionsCount);
            Task.WaitAll(tasks);
        }

        // task to execute query with new connection in a loop
        static async Task QueryExecutionTaskAsync(string connectionString, bool closeConnection, int times)
        {
            for (int i = 0; i < times; i++)
            {
                using (var conn = new SnowflakeDbConnection(connectionString))
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    using (DbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1, 2, 3";
                        try
                        {
                            using (DbDataReader reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                            {
                                while (await reader.ReadAsync().ConfigureAwait(false))
                                {
                                    for (int j = 0; j < reader.FieldCount; j++)
                                    {
                                        // Process each column as appropriate
                                        await reader.GetFieldValueAsync<object>(j).ConfigureAwait(false);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Assert.Fail("Caught unexpected exception: " + e);
                        }
                    }
                    if (closeConnection)
                    {
                        await conn.CloseAsync(new CancellationTokenSource().Token).ConfigureAwait(false);
                    }
                }
            }
        }

        // task to generate invalid(not finish open) connections in a loop
        static async Task InvalidConnectionTaskAsync(string connectionString, int times)
        {
            for (int i = 0; i < times; i++)
            {
                using (var conn = new SnowflakeDbConnection(connectionString))
                {
                    // intentionally not using await so the connection
                    // will be disposed with invalid underlying session
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    conn.OpenAsync();
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                }

                // wait 100ms each time so the invalid sessions are generated
                // roughly at the same speed as connections for query tasks
                await Task.Delay(100).ConfigureAwait(false);
            }
        }

    }
}
