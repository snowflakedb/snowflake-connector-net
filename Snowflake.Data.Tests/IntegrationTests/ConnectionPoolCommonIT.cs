/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data;
using System.Threading;
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
        private readonly PoolConfig _previousPoolConfig;

        public ConnectionPoolCommonIT(ConnectionPoolType connectionPoolTypeUnderTest)
        {
            _connectionPoolTypeUnderTest = connectionPoolTypeUnderTest;
            _previousPoolConfig = new PoolConfig();
        }

        [SetUp]
        public new void BeforeTest()
        {
            SnowflakeDbConnectionPool.SetConnectionPoolVersion(_connectionPoolTypeUnderTest);
            SnowflakeDbConnectionPool.ClearAllPools();
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                SnowflakeDbConnectionPool.SetPooling(true);
            }
            s_logger.Debug($"---------------- BeforeTest ---------------------");
            s_logger.Debug($"Testing Pool Type: {SnowflakeDbConnectionPool.GetConnectionPoolVersion()}");
        }

        [TearDown]
        public new void AfterTest()
        {
            _previousPoolConfig.Reset();
        }

        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
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
            SFBaseResultSet resultSet = statement.Execute(0, "select 1", null, false, false);
            Assert.AreEqual(true, resultSet.Next());
            Assert.AreEqual("1", resultSet.GetString(0));
            conn1.Close();
        }

        [Test]
        public void TestConnectionPoolWithDispose()
        {
            if (_connectionPoolTypeUnderTest == ConnectionPoolType.SingleConnectionCache)
            {
                SnowflakeDbConnectionPool.SetMaxPoolSize(1);
            }
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = "bad connection string";
            Assert.Throws<SnowflakeDbException>(() => conn1.Open());
            conn1.Close();
            Thread.Sleep(3000); // minPoolSize = 2 causes that another thread has been started. We sleep to make that thread finish.

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(0, SnowflakeDbConnectionPool.GetPool(conn1.ConnectionString).GetCurrentPoolSize());
        }

        [Test]
        public void TestFailWhenPreventingFromReturningToPoolNotOpenedConnection()
        {
            // arrange
            var connection = new SnowflakeDbConnection(ConnectionString);

            // act
            var thrown = Assert.Throws<Exception>(() => connection.PreventPooling());

            // assert
            Assert.That(thrown.Message, Does.Contain("Session not yet created for this connection. Unable to prevent the session from pooling"));
        }
    }
}
