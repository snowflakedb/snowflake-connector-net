using System;
using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Telemetry;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class TelemetryHelperTest
    {
        private const string ConnectionString = "account=testaccount;user=testuser;password=testpassword;";

        private SFSession CreateSessionWithTelemetry(MockTelemetryRestRequester requester)
        {
            var sessionContext = new SessionPropertiesContext { Password = null };
            var session = new SFSession(ConnectionString, sessionContext, requester);
            session.ProcessLoginResponse(new LoginResponse
            {
                data = new LoginResponseData
                {
                    token = "session_token",
                    masterToken = "master_token",
                    authResponseSessionInfo = new SessionInfo(),
                    nameValueParameter = new List<NameValueParameter>
                    {
                        new NameValueParameter
                        {
                            name = "CLIENT_TELEMETRY_ENABLED",
                            value = "true"
                        }
                    }
                },
                success = true
            });
            return session;
        }

        [Test]
        public void TestGenerateExceptionDataWithAllFields()
        {
            var ex = new SnowflakeDbException("42S02", 2003, "Table not found", "query-123");

            var data = TelemetryHelper.GenerateExceptionData(ex);

            Assert.AreEqual(TelemetryEventType.SqlException, data.Message[TelemetryField.Type]);
            Assert.AreEqual(SFEnvironment.DriverName, data.Message[TelemetryField.Source]);
            Assert.AreEqual(SFEnvironment.DriverName, data.Message[TelemetryField.DriverType]);
            Assert.AreEqual(SFEnvironment.DriverVersion, data.Message[TelemetryField.DriverVersion]);
            Assert.AreEqual("query-123", data.Message[TelemetryField.QueryId]);
            Assert.AreEqual("42S02", data.Message[TelemetryField.SqlState]);
            Assert.AreEqual("2003", data.Message[TelemetryField.ErrorNumber]);
            Assert.IsTrue(data.Message.ContainsKey(TelemetryField.Reason));
            Assert.IsFalse(data.Message.ContainsKey("ErrorMessage"));
            Assert.AreEqual("SnowflakeDbException", data.Message[TelemetryField.Exception]);
            Assert.IsNotNull(data.Timestamp);
        }

        [Test]
        public void TestGenerateExceptionDataWithMinimalFields()
        {
            var ex = new SnowflakeDbException(SFError.INTERNAL_ERROR, "something went wrong");

            var data = TelemetryHelper.GenerateExceptionData(ex);

            Assert.AreEqual(TelemetryEventType.SqlException, data.Message[TelemetryField.Type]);
            Assert.AreEqual(SFEnvironment.DriverName, data.Message[TelemetryField.DriverType]);
            Assert.IsFalse(data.Message.ContainsKey(TelemetryField.QueryId));
            Assert.IsTrue(data.Message.ContainsKey(TelemetryField.ErrorNumber));
            Assert.AreEqual("SnowflakeDbException", data.Message[TelemetryField.Exception]);
        }

        [Test]
        public void TestGenerateExceptionDataWithCustomEventType()
        {
            var ex = new SnowflakeDbException(SFError.REQUEST_TIMEOUT);

            var data = TelemetryHelper.GenerateExceptionData(ex, TelemetryEventType.HttpException);

            Assert.AreEqual(TelemetryEventType.HttpException, data.Message[TelemetryField.Type]);
        }

        [Test]
        public void TestGenerateExceptionDataMasksSecrets()
        {
            // Error message containing a password pattern (not a real credential)
            var ex = new SnowflakeDbException("", 1234, "Connection failed with password=s3cretP@ss", ""); // ggignore

            var data = TelemetryHelper.GenerateExceptionData(ex);

            // The secret detector should mask the password
            Assert.IsFalse(data.Message[TelemetryField.Reason].Contains("s3cretP@ss")); // ggignore
        }

        [Test]
        public void TestFilterStacktraceKeepsOnlySnowflakeFrames()
        {
            var nl = Environment.NewLine;
            var stacktrace =
                $"   at System.Environment.get_StackTrace(){nl}" +
                $"   at Snowflake.Data.Core.Telemetry.TelemetryHelper.FilterStacktrace(String stacktrace) in /Users/dev/repo/Snowflake.Data/Core/Telemetry/TelemetryHelper.cs:line 105{nl}" +
                $"   at MyApp.Program.Main(String[] args) in /Users/dev/myapp/Program.cs:line 10{nl}" +
                $"   at Snowflake.Data.Core.SFStatement.Execute() in /Users/dev/repo/Snowflake.Data/Core/SFStatement.cs:line 200{nl}";

            var filtered = TelemetryHelper.FilterStacktrace(stacktrace);

            // Should contain Snowflake.Data frames
            Assert.IsTrue(filtered.Contains("Snowflake.Data.Core.Telemetry.TelemetryHelper"));
            Assert.IsTrue(filtered.Contains("Snowflake.Data.Core.SFStatement"));
            // Should NOT contain user code or system frames
            Assert.IsFalse(filtered.Contains("MyApp.Program"));
            Assert.IsFalse(filtered.Contains("System.Environment"));
            // Should NOT contain user filesystem paths (neither app paths nor repo paths)
            Assert.IsFalse(filtered.Contains("/Users/dev/myapp"));
            Assert.IsFalse(filtered.Contains("/Users/dev/repo/"));
        }

        [Test]
        public void TestFilterStacktraceWithEmptyInput()
        {
            Assert.AreEqual(string.Empty, TelemetryHelper.FilterStacktrace(null));
            Assert.AreEqual(string.Empty, TelemetryHelper.FilterStacktrace(""));
        }

        [Test]
        public void TestSendExceptionTelemetryAddsToBuffer()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            var ex = new SnowflakeDbException("42S02", 2003, "Table not found", "query-123");

            TelemetryHelper.SendExceptionTelemetry(ex, session);

            Assert.AreEqual(1, session._telemetry.BufferSize);
        }

        [Test]
        public void TestSendExceptionTelemetryWithNullSessionDoesNotThrow()
        {
            var ex = new SnowflakeDbException(SFError.INTERNAL_ERROR, "error");

            Assert.DoesNotThrow(() => TelemetryHelper.SendExceptionTelemetry(ex, null));
        }

        [Test]
        public void TestSendExceptionTelemetryWithDisabledTelemetryDoesNotSend()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);
            session.ParameterMap[SFSessionParameter.CLIENT_TELEMETRY_ENABLED] = "false";

            var ex = new SnowflakeDbException(SFError.INTERNAL_ERROR, "error");

            TelemetryHelper.SendExceptionTelemetry(ex, session);

            Assert.AreEqual(0, session._telemetry.BufferSize);
        }

        [Test]
        public void TestSendExceptionTelemetryWithClosedTelemetryDoesNotThrow()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);
            session._telemetry.Close();

            var ex = new SnowflakeDbException(SFError.INTERNAL_ERROR, "error");

            Assert.DoesNotThrow(() => TelemetryHelper.SendExceptionTelemetry(ex, session));
        }

        [Test]
        public void TestSendAndThrowReturnsSameException()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            var ex = new SnowflakeDbException("42S02", 2003, "Table not found", "query-123");

            var returned = TelemetryHelper.SendAndThrow(ex, session);

            Assert.AreSame(ex, returned);
            Assert.AreEqual(1, session._telemetry.BufferSize);
        }

        [Test]
        public void TestSendAndThrowWithNullSession()
        {
            var ex = new SnowflakeDbException(SFError.INTERNAL_ERROR, "error");

            var returned = TelemetryHelper.SendAndThrow(ex, null);

            Assert.AreSame(ex, returned);
        }

        [Test]
        public void TestNoErrorLoopWhenTelemetrySendFails()
        {
            // If telemetry send itself fails, it should not trigger another telemetry event
            var requester = new MockTelemetryRestRequester { ThrowOnTelemetrySend = true };
            var session = CreateSessionWithTelemetry(requester);

            var ex = new SnowflakeDbException(SFError.INTERNAL_ERROR, "error");

            // First send - should disable telemetry due to failure
            session._telemetry.AddLog(new TelemetryData(
                new Dictionary<string, string> { { "type", "test" } },
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
            session._telemetry.SendBatch();

            // Now exception telemetry should be a no-op since telemetry is disabled
            Assert.DoesNotThrow(() => TelemetryHelper.SendExceptionTelemetry(ex, session));
            Assert.AreEqual(0, session._telemetry.BufferSize);
        }
    }
}
