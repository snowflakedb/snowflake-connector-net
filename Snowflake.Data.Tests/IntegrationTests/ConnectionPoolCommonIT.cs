/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture(ConnectionPoolType.SingleConnectionCache)]
    [TestFixture(ConnectionPoolType.MultipleConnectionPool)]
    [NonParallelizable]
    class ConnectionPoolCommonIT : SFBaseTest
    {
        private readonly ConnectionPoolType _connectionPoolTypeUnderTest;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ConnectionPoolManager>();
        private static PoolConfig s_previousPoolConfig;

        public ConnectionPoolCommonIT(ConnectionPoolType connectionPoolTypeUnderTest)
        {
            _connectionPoolTypeUnderTest = connectionPoolTypeUnderTest;
            s_previousPoolConfig = new PoolConfig();
        }

        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.SetConnectionPoolVersion(_connectionPoolTypeUnderTest);
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetPooling(true);
            s_logger.Debug($"---------------- BeforeTest ---------------------");
            s_logger.Debug($"Testing Pool Type: {SnowflakeDbConnectionPool.GetConnectionPoolVersion()}");
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
            const int ThreadNum = 12;
            // set short pooling timeout to cover the case that connection expired
            const int PoolTimeout = 3;

            // reset to default settings in case it changed by other test cases
            Assert.AreEqual(true, SnowflakeDbConnectionPool.GetPool(connectionString).GetPooling()); // to instantiate pool
            SnowflakeDbConnectionPool.SetMaxPoolSize(10);
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

        [Test]
        public void TestBasicConnectionPool()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);

            var conn1 = new SnowflakeDbConnection(ConnectionString);
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());
        }

        [Test]
        public void TestConnectionPool()
        {
            var conn1 = new SnowflakeDbConnection(ConnectionString);
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString;
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());

            conn2.Close();
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
        }

        [Test]
        public void TestConnectionPoolIsFull()
        {
            var pool = SnowflakeDbConnectionPool.GetPool(ConnectionString);
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString;
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = ConnectionString;
            conn3.Open();
            Assert.AreEqual(ConnectionState.Open, conn3.State);
            SnowflakeDbConnectionPool.ClearAllPools();
            pool = SnowflakeDbConnectionPool.GetPool(ConnectionString);
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);

            conn1.Close();
            Assert.AreEqual(1, pool.GetCurrentPoolSize());
            conn2.Close();
            Assert.AreEqual(2, pool.GetCurrentPoolSize());
            conn3.Close();
            Assert.AreEqual(2, pool.GetCurrentPoolSize());

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            Assert.AreEqual(ConnectionState.Closed, conn3.State);
            SnowflakeDbConnectionPool.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolExpirationWorks()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetTimeout(10);

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
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());
        }

        [Test]
        public void TestConnectionPoolMultiThreading()
        {
            Thread t1 = new Thread(() => ThreadProcess1(ConnectionString));
            Thread t2 = new Thread(() => ThreadProcess2(ConnectionString));

            t1.Start();
            t2.Start();

            t1.Join();
            t2.Join();
        }

        void ThreadProcess1(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            conn1.Open();
            Thread.Sleep(1000);
            conn1.Close();
            Thread.Sleep(4000);
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
        }

        void ThreadProcess2(string connstr)
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = connstr;
            conn1.Open();

            Thread.Sleep(5000);
            SFStatement statement = new SFStatement(conn1.SfSession);
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
            conn1.Close();
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetMaxPoolSize(0);
            SnowflakeDbConnectionPool.SetPooling(false);
        }

        [Test]
        public void TestConnectionPoolDisable()
        {
            var pool = SnowflakeDbConnectionPool.GetPool(ConnectionString);
            pool.SetPooling(false);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
        }

        [Test]
        public void TestConnectionPoolWithDispose()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = "bad connection string";
            Assert.Throws<SnowflakeDbException>(() => conn1.Open());
            conn1.Close();

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
        }

        [Test]
        public void TestConnectionPoolTurnOff()
        {
            SnowflakeDbConnectionPool.SetPooling(false);
            SnowflakeDbConnectionPool.SetPooling(true);
            SnowflakeDbConnectionPool.SetMaxPoolSize(1);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetPool(ConnectionString).GetCurrentPoolSize());
        }
    }
}
