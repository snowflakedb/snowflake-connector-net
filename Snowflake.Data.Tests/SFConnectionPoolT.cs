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
    using System.Diagnostics;
    using Snowflake.Data.Tests.Mock;
    using System.Runtime.InteropServices;

    [TestFixture]
    class SFConnectionPoolT : SFBaseTest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionPoolT>();

        [Test]
        public void TestBasicConnectionPool()
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);
            conn1.Close();

            Assert.AreEqual(ConnectionState.Open, conn1.State);
            SnowflakeDbConnection.ClearAllPools();
        }

        [Test]
        public void TestReuseConnectionPool()
        {
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString;
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);

            conn1.Close();
            conn2.Close();

            Assert.AreEqual(ConnectionState.Open, conn1.State);
            Assert.AreEqual(ConnectionState.Open, conn2.State);
            SnowflakeDbConnection.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolIsFull()
        {
            SnowflakeDbConnection.ClearAllPools();
            SnowflakeDbConnection.SetMaxPoolSize(2);
            
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
            conn3.Close();

            Assert.AreEqual(ConnectionState.Open, conn1.State);
            Assert.AreEqual(ConnectionState.Open, conn2.State);
            Assert.AreEqual(ConnectionState.Closed, conn3.State); //out of the pool, so connection state is closed
            SnowflakeDbConnection.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolClean()
        {
            SnowflakeDbConnection.ClearAllPools();
            SnowflakeDbConnection.SetMaxPoolSize(2);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString + " retryCount=1";
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);

            conn2.Close();
            SnowflakeDbConnection.ClearPool(conn2);

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = ConnectionString + "  retryCount=2";
            conn3.Open();
            Assert.AreEqual(ConnectionState.Open, conn3.State);


            conn1.Close();
            conn3.Close();

            Assert.AreEqual(ConnectionState.Open, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            Assert.AreEqual(ConnectionState.Open, conn3.State);
            SnowflakeDbConnection.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolCleanNonActive()
        {
            SnowflakeDbConnection.ClearAllPools();
            SnowflakeDbConnection.SetMaxPoolSize(2);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString + " retryCount=1";
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);

            conn1.Close();

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = ConnectionString + "  retryCount=2";
            conn3.Open();
            Assert.AreEqual(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = ConnectionString + "  retryCount=3";
            conn4.Open();
            Assert.AreEqual(ConnectionState.Open, conn4.State);

            conn2.Close();
            conn3.Close();
            conn4.Close();

            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(ConnectionState.Open, conn2.State);
            Assert.AreEqual(ConnectionState.Open, conn3.State);
            Assert.AreEqual(ConnectionState.Closed, conn4.State);
            SnowflakeDbConnection.ClearAllPools();
        }

        [Test]
        public void TestConnectionPoolMinPoolSize()
        {
            SnowflakeDbConnection.ClearAllPools();
            SnowflakeDbConnection.SetMinPoolSize(1);
            SnowflakeDbConnection.SetMaxPoolSize(2);

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
            conn1.Open();
            Assert.AreEqual(ConnectionState.Open, conn1.State);

            var conn2 = new SnowflakeDbConnection();
            conn2.ConnectionString = ConnectionString + " retryCount=1";
            conn2.Open();
            Assert.AreEqual(ConnectionState.Open, conn2.State);

            conn1.Close();
            conn2.Close();

            var conn3 = new SnowflakeDbConnection();
            conn3.ConnectionString = ConnectionString + "  retryCount=2";
            conn3.Open();
            Assert.AreEqual(ConnectionState.Open, conn3.State);

            var conn4 = new SnowflakeDbConnection();
            conn4.ConnectionString = ConnectionString + "  retryCount=3";
            conn4.Open();
            Assert.AreEqual(ConnectionState.Open, conn4.State);

            conn3.Close();
            conn4.Close();

            Assert.AreEqual(ConnectionState.Open, conn1.State);
            Assert.AreEqual(ConnectionState.Closed, conn2.State);
            Assert.AreEqual(ConnectionState.Open, conn3.State);
            Assert.AreEqual(ConnectionState.Closed, conn4.State);
            SnowflakeDbConnection.ClearAllPools();
        }
    }
}

        