/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Tests.Util;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Mock;
using Moq;
using NUnit.Framework;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture, NonParallelizable]
    class SFConnectionPoolITAsync : SFBaseTestAsync
    {
        private static PoolConfig s_previousPoolConfigRestorer;

        [OneTimeSetUp]
        public static void BeforeAllTests()
        {
            s_previousPoolConfigRestorer = new PoolConfig();
        }

        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [TearDown]
        public new void AfterTest()
        {
            s_previousPoolConfigRestorer.Reset();
        }
        
        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolWithAsync()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                SnowflakeDbConnectionPool.SetMaxPoolSize(1);

                int timeoutSec = 0;
                string infiniteLoginTimeOut = $";connection_timeout={timeoutSec}";

                conn.ConnectionString = infiniteLoginTimeOut;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                try
                {
                    conn.OpenAsync(connectionCancelToken.Token);
                }
                catch (SnowflakeDbException ex)
                {
                    conn.CloseAsync(connectionCancelToken.Token);
                }

                Thread.Sleep(10 * 1000);
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
            string connStr = ConnectionString + ";application=conn_pool_test_invalid_openasync";
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

        [Test]
        // test connection pooling with concurrent connection using async calls
        public void TestConcurrentConnectionPoolingAsync()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = ConnectionString + ";application=TestConcurrentConnectionPoolingAsync";
            ConcurrentPoolingAsyncHelper(connStr, true);
        }

        [Test]
        public void TestRollbackTransactionOnPooledWhenExceptionOccurred()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);

            object firstOpenedSessionId;
            using (var connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                firstOpenedSessionId = connection.SfSession.sessionId;
                connection.BeginTransaction();
                Assert.AreEqual(true, connection.HasActiveExplicitTransaction());
                Assert.Throws<SnowflakeDbException>(() =>
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "invalid command will throw exception and leave session with an unfinished transaction";
                        command.ExecuteNonQuery();
                    }
                });
            }

            using (var connectionWithSessionReused = new SnowflakeDbConnection())
            {
                connectionWithSessionReused.ConnectionString = ConnectionString;
                connectionWithSessionReused.Open();
                
                Assert.AreEqual(firstOpenedSessionId, connectionWithSessionReused.SfSession.sessionId);
                Assert.AreEqual(false, connectionWithSessionReused.HasActiveExplicitTransaction());
                using (var cmd = connectionWithSessionReused.CreateCommand())
                {
                    cmd.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.AreEqual(DBNull.Value, cmd.ExecuteScalar());
                }
            }
            
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be reused and any pending transaction rolled back before it gets back to the pool");
        }

        [Test]
        public void TestTransactionStatusNotTrackedForNonExplicitTransactionCalls()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            using (var connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "BEGIN"; // in general can be put as a part of a multi statement call and mixed with commit as well 
                    command.ExecuteNonQuery();
                    Assert.AreEqual(false, connection.HasActiveExplicitTransaction()); 
                }
            }
        }

        [Test]
        public void TestRollbackTransactionOnPooledWhenConnectionClose()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be returned to the pool");

            string firstOpenedSessionId;
            using (var connection1 = new SnowflakeDbConnection())
            {
                connection1.ConnectionString = ConnectionString;
                connection1.Open();
                Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection session is added to the pool after close connection");
                connection1.BeginTransaction();
                Assert.AreEqual(true, connection1.HasActiveExplicitTransaction());
                using (var command = connection1.CreateCommand())
                {
                    firstOpenedSessionId = connection1.SfSession.sessionId;
                    command.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.AreNotEqual(DBNull.Value, command.ExecuteScalar());
                }
            }
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be returned to the pool");

            using (var connection2 = new SnowflakeDbConnection())
            {
                connection2.ConnectionString = ConnectionString;
                connection2.Open();
                Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection session should be now removed from the pool");
                Assert.AreEqual(false, connection2.HasActiveExplicitTransaction());
                using (var command = connection2.CreateCommand())
                {
                    Assert.AreEqual(firstOpenedSessionId, connection2.SfSession.sessionId);
                    command.CommandText = "SELECT CURRENT_TRANSACTION()";
                    Assert.AreEqual(DBNull.Value, command.ExecuteScalar());
                }
            }
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be returned to the pool");
        }

        [Test]
        public void TestFailureOfTransactionRollbackOnConnectionClosePreventsAddingToPool()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            var commandThrowingExceptionOnlyForRollback = new Mock<SnowflakeDbCommand>();
            commandThrowingExceptionOnlyForRollback.CallBase = true;
            commandThrowingExceptionOnlyForRollback.SetupSet(it => it.CommandText = "ROLLBACK")
                .Throws(new SnowflakeDbException(SFError.INTERNAL_ERROR, "Unexpected failure on transaction rollback when connection is returned to the pool with pending transaction"));
            var mockDbProviderFactory = new Mock<DbProviderFactory>();
            mockDbProviderFactory.Setup(p => p.CreateCommand()).Returns(commandThrowingExceptionOnlyForRollback.Object);

            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            using (var connection = new TestSnowflakeDbConnection(mockDbProviderFactory.Object))
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                connection.BeginTransaction();
                Assert.AreEqual(true, connection.HasActiveExplicitTransaction());
                // no Rollback or Commit; during internal Rollback while closing a connection a mocked exception will be thrown
            }
            
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Should not return connection to the pool");
        }

        [Test]
        // test connection pooling with concurrent connection and using async calls no close
        // call for connection. Connection is closed when Dispose() is called
        // by framework.
        public void TestConcurrentConnectionPoolingDisposeAsync()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = ConnectionString + ";application=TestConcurrentConnectionPoolingDisposeAsync";
            ConcurrentPoolingAsyncHelper(connStr, false);
        }

        static void ConcurrentPoolingAsyncHelper(string connectionString, bool closeConnection)
        {
            // task number a bit larger than pool size so some connections
            // would fail on pooling while some connections could success
            const int TaskNum = 12;
            // set short pooling timeout to cover the case that connection expired
            const int PoolTimeout = 3;

            // reset to default settings in case it changed by other test cases
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            SnowflakeDbConnectionPool.SetTimeout(PoolTimeout);

            var tasks = new Task[TaskNum + 1];
            for (int i = 0; i < TaskNum; i++)
            {
                tasks[i] = QueryExecutionTaskAsync(connectionString, closeConnection);
            }
            // cover the case of invalid sessions to ensure that won't
            // break connection pooling
            tasks[TaskNum] = InvalidConnectionTaskAsync(connectionString);
            Task.WaitAll(tasks);

            // set pooling timeout back to default to avoid impact on other test cases
            SnowflakeDbConnectionPool.SetTimeout(3600);
        }

        // task to execute query with new connection in a loop
        static async Task QueryExecutionTaskAsync(string connectionString, bool closeConnection)
        {
            for (int i = 0; i < 100; i++)
            {
                using (var conn = new SnowflakeDbConnection(connectionString))
                {
                    await conn.OpenAsync();
                    using (DbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1, 2, 3";
                        try
                        {
                            using (DbDataReader reader = await cmd.ExecuteReaderAsync())
                            {
                                while (await reader.ReadAsync())
                                {
                                    for (int j = 0; j < reader.FieldCount; j++)
                                    {
                                        // Process each column as appropriate
                                        await reader.GetFieldValueAsync<object>(j);
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
                        await conn.CloseAsync(new CancellationTokenSource().Token);
                    }
                }
            }
        }

        // task to generate invalid(not finish open) connections in a loop
        static async Task InvalidConnectionTaskAsync(string connectionString)
        {
            for (int i = 0; i < 100; i++)
            {
                using (var conn = new SnowflakeDbConnection(connectionString))
                {
                    // intentionally not using await so the connection
                    // will be disposed with invalid underlying session
                    conn.OpenAsync();
                };
                // wait 100ms each time so the invalid sessions are generated
                // roughly at the same speed as connections for query tasks
                await Task.Delay(100);
            }
        }
        
        private class TestSnowflakeDbConnection : SnowflakeDbConnection
        {
            public TestSnowflakeDbConnection(DbProviderFactory dbProviderFactory)
            {
                DbProviderFactory = dbProviderFactory;
            }

            protected override DbProviderFactory DbProviderFactory { get; }
        }
    }
}
