/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using System;
    using Snowflake.Data.Core;
    using System.Threading.Tasks;
    using System.Threading;
    using Snowflake.Data.Log;
    using Snowflake.Data.Tests.Mock;
    using System.Data.Common;
    using Moq;
    
    class PoolConfig {
        private readonly bool _pooling;
        private readonly long _timeout;
        private readonly int _maxPoolSize;

        public PoolConfig()
        {
            _maxPoolSize = SnowflakeDbConnectionPool.GetMaxPoolSize();
            _timeout = SnowflakeDbConnectionPool.GetTimeout();
            _pooling = SnowflakeDbConnectionPool.GetPooling();
        }

        public void Reset()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(_maxPoolSize);
            SnowflakeDbConnectionPool.SetTimeout(_timeout);
            SnowflakeDbConnectionPool.SetPooling(_pooling);
        }
    }
    
    [TestFixture, NonParallelizable]
    class SFConnectionPoolT : SFBaseTest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionPoolT>();
        private static readonly PoolConfig previousPoolConfig = new PoolConfig();

        [SetUp]
        public void BeforeTest()
        {
            previousPoolConfig.Reset();
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [TearDown]
        public void AfterTest()
        {
            previousPoolConfig.Reset();
        }

        [Test]
        [Ignore("dummy test case for showing test progress.")]
        public void ConnectionPoolTDone()
        {
            // Do nothing;
        }

        [Test]
        // test connection pooling with concurrent connection
        public void TestConcurrentConnectionPooling()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = ConnectionString + ";application=TestConcurrentConnectionPooling";
            ConcurrentPoolingHelper(connStr, true);
        }

        [Test]
        // test connection pooling with concurrent connection and no close
        // call for connection. Connection is closed when Dispose() is called
        // by framework.
        public void TestConcurrentConnectionPoolingDispose()
        {
            // add test case name in connection string to make in unique for each test case
            string connStr = ConnectionString + ";application=TestConcurrentConnectionPoolingNoClose";
            ConcurrentPoolingHelper(connStr, false);
        }

        static void ConcurrentPoolingHelper(string connectionString, bool closeConnection)
        {
            // thread number a bit larger than pool size so some connections
            // would fail on pooling while some connections could success
            const int threadNum = 12;
            // set short pooling timeout to cover the case that connection expired
            const int poolTimeout = 3;

            // reset to default settings in case it changed by other test cases
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetTimeout(poolTimeout);

            var threads = new Task[threadNum];
            for (int i = 0; i < threadNum; i++)
            {
                threads[i] = Task.Factory.StartNew(() =>
                {
                    QueryExecutionThread(connectionString, closeConnection);
                });
            }
            Task.WaitAll(threads);
            // set pooling timeout back to default to avoid impact on other test cases
            SnowflakeDbConnectionPool.SetTimeout(3600);
        }

        // thead to execute query with new connection in a loop
        static void QueryExecutionThread(string connectionString, bool closeConnection)
        {
            for (int i = 0; i < 100; i++)
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
                                        object obj = reader.GetFieldValue<object>(j);
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
                };
            }
        }

        [Test]
        public void TestBasicConnectionPool()
        {
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            SnowflakeDbConnectionPool.ClearAllPools();

            var conn1 = new SnowflakeDbConnection(ConnectionString);
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestConnectionPool()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            var conn1 = new SnowflakeDbConnection(ConnectionString);
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString;
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            conn2.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolIsFull()
        {
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.ClearAllPools();
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
            SnowflakeDbConnectionPool.ClearAllPools();

            conn1.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            conn2.Close();
            Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            conn3.Close();
            Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            Assert.AreEqual(ConnectionState.Closed, conn3.State);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolExpirationWorks()
        {
            System.Threading.Thread.Sleep(10000); // wait for 10 seconds, in case other test still running.
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetTimeout(10);
            SnowflakeDbConnectionPool.SetPooling(true);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;

            conn1.Open();
            conn1.Close();
            SnowflakeDbConnectionPool.SetTimeout(-1);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString;
            conn2.Open();
            conn2.Close();
            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = ConnectionString;
            conn3.Open();
            conn3.Close();

            // The pooling timeout should apply to all connections being pooled,
            // not just the connections created after the new setting,
            // so expected result should be 0
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            SnowflakeDbConnectionPool.SetPooling(false);
        }

        [Test]
        public void TestConnectionPoolClean()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
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
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolFull()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetPooling(true);

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
        public void TestConnectionPoolMultiThreading()
        {
            Thread t1 = new Thread(() => ThreadProcess1(ConnectionString));
            Thread t2 = new Thread(() => ThreadProcess2(ConnectionString));

            t1.Start();
            t2.Start();
        }

        static void ThreadProcess1(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            conn1.Open();
            Thread.Sleep(1000);
            conn1.Close();
            Thread.Sleep(4000);
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
        }

        static void ThreadProcess2(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            conn1.Open();

            Thread.Sleep(5000);
            SFStatement statement = new SFStatement(conn1.SfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetMaxPoolSize(0);
            SnowflakeDbConnectionPool.SetPooling(false);
        }

        [Test]
        public void TestConnectionPoolDisable()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetPooling(false);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestConnectionPoolWithDispose()
        {
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            SnowflakeDbConnectionPool.ClearAllPools();

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = "";
            try
            {
                conn1.Open();
            }
            catch (SnowflakeDbException ex)
            {
                Console.WriteLine($"connection failed:" + ex);
                conn1.Close();
            }

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestConnectionPoolTurnOff()
        {
            SnowflakeDbConnectionPool.SetPooling(false);
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            SnowflakeDbConnectionPool.ClearAllPools();

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            SnowflakeDbConnectionPool.SetPooling(false);
            //Put a breakpoint at SFSession close function, after connection pool is off, it will send close session request.
        }
    }

    [TestFixture]
    class SFConnectionPoolITAsync : SFBaseTestAsync
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionPoolITAsync>();
        private static readonly PoolConfig previousPoolConfig = new PoolConfig();

        [SetUp]
        public void BeforeTest()
        {
            previousPoolConfig.Reset();
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [TearDown]
        public void AfterTest()
        {
            previousPoolConfig.Reset();
        }

        [Test]
        public void TestConnectionPoolWithAsync()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                SnowflakeDbConnectionPool.SetMaxPoolSize(1);
                SnowflakeDbConnectionPool.ClearAllPools();

                int timeoutSec = 0;
                string infiniteLoginTimeOut = String.Format("" + ";connection_timeout={0}",
                    timeoutSec);

                conn.ConnectionString = infiniteLoginTimeOut;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                try
                {
                    Task connectTask = conn.OpenAsync(connectionCancelToken.Token);
                }
                catch (SnowflakeDbException ex)
                {
                    Console.WriteLine($"connection failed:" + ex);
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
                                object obj = reader.GetFieldValue<object>(i);
                            }
                        }
                    }
                }
                catch (SnowflakeDbException ex)
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

            object firstOpenedSessionId = null;
            using (var connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                firstOpenedSessionId = connection.SfSession.sessionId;
                connection.BeginTransaction();
                Assert.AreEqual(true, connection.HasActiveTransaction());
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
                Assert.AreEqual(false, connectionWithSessionReused.HasActiveTransaction());
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
                    Assert.AreEqual(false, connection.HasActiveTransaction()); 
                }
            }
        }

        [Test]
        public void TestRollbackTransactionOnPooledWhenConnectionClose()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection should be returned to the pool");

            string firstOpenedSessionId = null;
            using (var connection1 = new SnowflakeDbConnection())
            {
                connection1.ConnectionString = ConnectionString;
                connection1.Open();
                Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize(), "Connection session is added to the pool after close connection");
                connection1.BeginTransaction();
                Assert.AreEqual(true, connection1.HasActiveTransaction());
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
                Assert.AreEqual(false, connection2.HasActiveTransaction());
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
            SnowflakeDbConnectionPool.SetPooling(true);
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
                Assert.AreEqual(true, connection.HasActiveTransaction());
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
            const int taskNum = 12;
            // set short pooling timeout to cover the case that connection expired
            const int poolTimeout = 3;

            // reset to default settings in case it changed by other test cases
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetTimeout(poolTimeout);

            var tasks = new Task[taskNum + 1];
            for (int i = 0; i < taskNum; i++)
            {
                tasks[i] = QueryExecutionTaskAsync(connectionString, closeConnection);
            }
            // cover the case of invalid sessions to ensure that won't
            // break connection pooling
            tasks[taskNum] = InvalidConnectionTaskAsync(connectionString);
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
                                        object obj = await reader.GetFieldValueAsync<object>(j);
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
                };
            }
        }

        // task to generate invalid(not finish open) connections in a loop
        static async Task InvalidConnectionTaskAsync(string connectionString)
        {
            for (int i = 0; i < 100; i++)
            {
                using (var conn = new SnowflakeDbConnection(connectionString))
                {
                    // intentially not using await so the connection
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
