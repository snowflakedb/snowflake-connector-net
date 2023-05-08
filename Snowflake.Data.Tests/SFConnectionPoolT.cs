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
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
        public void ConnectionPoolTDone()
        {
            // Do nothing;
        }

        [Test]
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
        public void TestBasicConnectionPool()
        {
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
        }

        [Test]
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
        public void TestConnectionPool()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString;
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
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
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

            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            SnowflakeDbConnectionPool.SetPooling(false);
        }

        [Test]
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
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
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
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
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
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
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
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
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
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
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
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

        [Test]
        [Ignore("Disable test case to prevent the static variable changed at the same time.")]
        public void TestConnectionPoolWithAsync()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                SnowflakeDbConnectionPool.SetPooling(true);
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
    }
}

        