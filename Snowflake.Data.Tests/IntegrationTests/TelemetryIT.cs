using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Telemetry;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    class TelemetryIT : SFBaseTest
    {
        private TelemetryData CreateTestLog(string eventType = TelemetryEventType.SqlException)
        {
            return new TelemetryData(
                new Dictionary<string, string>
                {
                    { TelemetryField.Type, eventType },
                    { TelemetryField.DriverType, SFEnvironment.DriverName },
                    { TelemetryField.DriverVersion, SFEnvironment.DriverVersion },
                    { TelemetryField.Reason, "integration test telemetry event" }
                },
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            );
        }

        [Test]
        public void TestTelemetryClientCreatedOnConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                Assert.IsNotNull(conn.SfSession._telemetry);
                Assert.IsFalse(conn.SfSession._telemetry.IsClosed);

                conn.Close();
            }
        }

        [Test]
        public void TestSendBatchToServer()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                var telemetry = conn.SfSession._telemetry;

                telemetry.AddLog(CreateTestLog());
                telemetry.AddLog(CreateTestLog());

                Assert.AreEqual(2, telemetry.BufferSize);

                var result = telemetry.SendBatch();

                Assert.IsTrue(result);
                Assert.AreEqual(0, telemetry.BufferSize);
            }
        }

        [Test]
        public void TestSendBatchAsyncToServer()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                var telemetry = conn.SfSession._telemetry;

                telemetry.AddLog(CreateTestLog());
                telemetry.AddLog(CreateTestLog());

                var result = Task.Run(async () => await telemetry.SendBatchAsync().ConfigureAwait(false)).Result;

                Assert.IsTrue(result);
                Assert.AreEqual(0, telemetry.BufferSize);
            }
        }

        [Test]
        public void TestCloseFlushesToServer()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                var telemetry = conn.SfSession._telemetry;

                telemetry.AddLog(CreateTestLog());
                telemetry.AddLog(CreateTestLog());
                telemetry.AddLog(CreateTestLog());

                Assert.AreEqual(3, telemetry.BufferSize);

                // Close should flush
                telemetry.Close();

                Assert.IsTrue(telemetry.IsClosed);
                Assert.AreEqual(0, telemetry.BufferSize);
            }
        }

        [Test]
        public void TestAutoFlushAtThreshold()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                // Close original telemetry client before replacing
                conn.SfSession._telemetry.Close();
                // Replace with small flush size for testing
                var telemetry = new TelemetryClient(conn.SfSession, conn.SfSession.restRequester, 5);
                conn.SfSession._telemetry = telemetry;

                for (int i = 0; i < 5; i++)
                {
                    telemetry.AddLog(CreateTestLog());
                }

                // Wait for background async flush to complete
                SpinWait.SpinUntil(() => telemetry.BufferSize == 0, TimeSpan.FromSeconds(5));

                Assert.AreEqual(0, telemetry.BufferSize);
            }
        }

        [Test]
        public void TestConnectionCloseFlushTelemetry()
        {
            // Verify that closing a connection doesn't throw due to telemetry flush
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                var telemetry = conn.SfSession._telemetry;
                telemetry.AddLog(CreateTestLog());
                telemetry.AddLog(CreateTestLog());

                Assert.DoesNotThrow(() => conn.Close());
            }
        }

        [Test]
        public void TestAddLogAfterCloseDoesNotThrow()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                var telemetry = conn.SfSession._telemetry;
                telemetry.Close();

                Assert.DoesNotThrow(() => telemetry.AddLog(CreateTestLog()));
                Assert.AreEqual(0, telemetry.BufferSize);
            }
        }

        [Test]
        public void TestDoubleCloseDoesNotThrow()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                var telemetry = conn.SfSession._telemetry;

                Assert.DoesNotThrow(() =>
                {
                    telemetry.Close();
                    telemetry.Close();
                });
            }
        }

        [Test]
        public void TestTelemetryEnabledByDefault()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                Assert.IsTrue(conn.SfSession._telemetry.IsTelemetryEnabled());
            }
        }

        [Test]
        public void TestFailedQueryGeneratesTelemetryEvent()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                var telemetry = conn.SfSession._telemetry;
                var bufferBefore = telemetry.BufferSize;

                try
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = "SELECT * FROM nonexistent_table_for_telemetry_test_xyz";
                    cmd.ExecuteReader();
                    Assert.Fail("Expected exception was not thrown");
                }
                catch (SnowflakeDbException)
                {
                    // Expected - the query should fail
                }

                // The failed query should have generated a telemetry event
                Assert.Greater(telemetry.BufferSize, bufferBefore);
            }
        }
    }
}
