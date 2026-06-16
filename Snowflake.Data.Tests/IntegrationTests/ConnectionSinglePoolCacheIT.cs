using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
using Moq;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public class ConnectionSinglePoolCacheIT : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;

        public ConnectionSinglePoolCacheIT(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixture = fixture;
            ConnectionManagerTestsFacade.RegisterDedicatedContext(this, ConnectionPoolType.SingleConnectionCache);
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetPooling(true);
        }

        public void Dispose()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            ConnectionManagerTestsFacade.UnregisterDedicatedContext(this);
        }

        [SFFact]
        public void TestConcurrentConnectionPooling()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPooling;";
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            ConcurrentPoolingHelper(connStr, true);
        }

        [SFFact]
        // test connection pooling with concurrent connection and no close
        // call for connection. Connection is closed when Dispose() is called
        // by framework.
        public void TestConcurrentConnectionPoolingDispose()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = _fixture.ConnectionString + ";application=TestConcurrentConnectionPoolingNoClose;";
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            ConcurrentPoolingHelper(connStr, false);
        }

        [SFFact]
        public async Task TestPutConnectionToPoolOnClose()
        {
            // arrange
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                Assert.Equal(conn.State, ConnectionState.Closed);
                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                await conn.OpenAsync(connectionCancelToken.Token).ConfigureAwait(false);

                // act
                conn.Close();

                // assert
                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            }
        }

        [SFFact]
        public async Task TestDoNotPutInvalidConnectionToPool()
        {
            // arrange
            var invalidConnectionString = ";connection_timeout=0";
            using (var conn = new SnowflakeDbConnection(invalidConnectionString))
            {
                Assert.Equal(conn.State, ConnectionState.Closed);
                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                try
                {
                    await conn.OpenAsync(connectionCancelToken.Token).ConfigureAwait(false);
                    Assert.Fail("OpenAsync should throw exception");
                }
                catch { }

                // act
                conn.Close();

                // assert
                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            }
        }

        [SFFact]
        public async Task TestConnectionPoolWithInvalidOpen()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            // make the connection string unique so it won't pick up connection
            // pooled by other test cases.
            string connStr = _fixture.ConnectionString + "application=conn_pool_test_invalid_openasync";
            using (var connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = connStr;
                // call openAsync but do not wait and destroy it direct
                // so the session is initialized with empty token
                connection.Open();
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
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
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


        private static void ConcurrentPoolingHelper(string connectionString, bool closeConnection)
        {
            // thread number a bit larger than pool size so some connections
            // would fail on pooling while some connections could success
            const int ThreadNum = 3;
            // set short pooling timeout to cover the case that connection expired
            const int PoolTimeout = 1;

            // reset to default settings in case it changed by other test cases
            Assert.True(SnowflakeDbConnectionPool.GetPool(connectionString).GetPooling()); // to instantiate pool
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetTimeout(PoolTimeout);

            var threads = new Task[ThreadNum];
            for (int i = 0; i < ThreadNum; i++)
            {
                threads[i] = Task.Factory.StartNew(() =>
                {
                    QueryExecutionThread(connectionString, closeConnection);
                });
            }
            Task.WaitAll(threads);
        }

        // thead to execute query with new connection in a loop
        private static void QueryExecutionThread(string connectionString, bool closeConnection)
        {
            for (int i = 0; i < 10; i++)
            {
                using (DbConnection conn = new SnowflakeDbConnection(connectionString))
                {
                    conn.Open();
                    using (DbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1, 2, 3";
                        try
                        {
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    for (int j = 0; j < reader.FieldCount; j++)
                                    {
                                        // Process each column as appropriate
                                        reader.GetFieldValue<object>(j);
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
                        conn.Close();
                    }
                }
            }
        }
    }
}
