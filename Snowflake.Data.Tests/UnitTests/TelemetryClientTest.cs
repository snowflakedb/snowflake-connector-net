using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Telemetry;

namespace Snowflake.Data.Tests.UnitTests
{
    /// <summary>
    /// A mock rest requester that captures telemetry POST requests for assertion.
    /// Returns a configurable response for telemetry sends, and a successful
    /// login response for session setup.
    /// </summary>
    internal class MockTelemetryRestRequester : IMockRestRequester
    {
        internal ConcurrentBag<TelemetryRequest> CapturedTelemetryRequests { get; } = new ConcurrentBag<TelemetryRequest>();
        internal bool TelemetryResponseSuccess { get; set; } = true;
        internal bool ThrowOnTelemetrySend { get; set; } = false;

        public T Post<T>(IRestRequest request)
        {
            return Task.Run(async () => await PostAsync<T>(request, CancellationToken.None).ConfigureAwait(false)).Result;
        }

        public Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)postRequest;

            if (sfRequest.jsonBody is LoginRequest)
            {
                LoginResponse authnResponse = new LoginResponse
                {
                    data = new LoginResponseData()
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
                };
                return Task.FromResult<T>((T)(object)authnResponse);
            }

            if (sfRequest.Url.AbsolutePath.Contains("/telemetry/send"))
            {
                if (ThrowOnTelemetrySend)
                {
                    throw new Exception("Simulated telemetry send failure");
                }

                if (sfRequest.jsonBody is TelemetryRequest telemetryReq)
                {
                    CapturedTelemetryRequests.Add(telemetryReq);
                }

                var response = new NullDataResponse { success = TelemetryResponseSuccess };
                return Task.FromResult<T>((T)(object)response);
            }

            // Close session
            if (typeof(T) == typeof(CloseResponse))
            {
                return Task.FromResult<T>((T)(object)new CloseResponse { success = true });
            }

            throw new NotImplementedException($"Unexpected POST request: {sfRequest.Url}");
        }

        public T Get<T>(IRestRequest request) => default;
        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult<T>(default);
        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken) => Task.FromResult<HttpResponseMessage>(null);
        public HttpResponseMessage Get(IRestRequest request) => null;
        public void setHttpClient(HttpClient httpClient) { }
    }

    [TestFixture]
    class TelemetryClientTest
    {
        private const string ConnectionString = "account=testaccount;user=testuser;password=testpassword;";

        private SFSession CreateSessionWithTelemetry(MockTelemetryRestRequester requester, int flushSize = TelemetryClient.DefaultFlushSize)
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

            // Replace with a custom flush size if needed
            if (flushSize != TelemetryClient.DefaultFlushSize)
            {
                session._telemetry = new TelemetryClient(session, requester, flushSize);
            }

            return session;
        }

        private TelemetryData CreateTestTelemetryData(string eventType = TelemetryEventType.SqlException)
        {
            return new TelemetryData(
                new Dictionary<string, string>
                {
                    { TelemetryField.Type, eventType },
                    { TelemetryField.DriverType, SFEnvironment.DriverName },
                    { TelemetryField.DriverVersion, SFEnvironment.DriverVersion },
                    { TelemetryField.Reason, "test error" }
                },
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            );
        }

        [Test]
        public void TestTelemetryClientCreatedOnLogin()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            Assert.IsNotNull(session._telemetry);
            Assert.IsFalse(session._telemetry.IsClosed);
        }

        [Test]
        public void TestAddLogAccumulatesInBuffer()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.AddLog(CreateTestTelemetryData());

            Assert.AreEqual(3, session._telemetry.BufferSize);
            Assert.AreEqual(0, requester.CapturedTelemetryRequests.Count);
        }

        [Test]
        public void TestSendBatchClearsBufferAndSends()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.AddLog(CreateTestTelemetryData());

            var result = session._telemetry.SendBatch();

            Assert.IsTrue(result);
            Assert.AreEqual(0, session._telemetry.BufferSize);
            Assert.AreEqual(1, requester.CapturedTelemetryRequests.Count);
            Assert.AreEqual(2, requester.CapturedTelemetryRequests.First().Logs.Count);
        }

        [Test]
        public void TestSendBatchPayloadFormat()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            var data = CreateTestTelemetryData();
            session._telemetry.AddLog(data);
            session._telemetry.SendBatch();

            var captured = requester.CapturedTelemetryRequests.First();
            var log = captured.Logs[0];

            Assert.AreEqual(TelemetryEventType.SqlException, log.Message[TelemetryField.Type]);
            Assert.AreEqual(SFEnvironment.DriverName, log.Message[TelemetryField.DriverType]);
            Assert.AreEqual(SFEnvironment.DriverVersion, log.Message[TelemetryField.DriverVersion]);
            Assert.AreEqual("test error", log.Message[TelemetryField.Reason]);
            Assert.IsNotNull(log.Timestamp);
            Assert.Greater(log.Timestamp, 0);
        }

        [Test]
        public void TestAutoFlushAtThreshold()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester, flushSize: 5);

            for (int i = 0; i < 5; i++)
            {
                session._telemetry.AddLog(CreateTestTelemetryData());
            }

            // Wait for background async flush to complete (generous timeout for CI)
            var flushed = SpinWait.SpinUntil(() => requester.CapturedTelemetryRequests.Count > 0, TimeSpan.FromSeconds(10));

            Assert.IsTrue(flushed, "Auto-flush did not trigger within timeout");
            Assert.AreEqual(0, session._telemetry.BufferSize);
            Assert.AreEqual(5, requester.CapturedTelemetryRequests.First().Logs.Count);
        }

        [Test]
        public void TestCloseFlushesRemainingLogs()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.Close();

            Assert.IsTrue(session._telemetry.IsClosed);
            Assert.AreEqual(1, requester.CapturedTelemetryRequests.Count);
            Assert.AreEqual(2, requester.CapturedTelemetryRequests.First().Logs.Count);
        }

        [Test]
        public void TestSelfDisablesOnSendFailure()
        {
            var requester = new MockTelemetryRestRequester { TelemetryResponseSuccess = false };
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.SendBatch();

            // After failure, telemetry should be disabled
            Assert.IsFalse(session._telemetry.IsTelemetryEnabled());

            // Subsequent logs should be silently ignored
            session._telemetry.AddLog(CreateTestTelemetryData());
            Assert.AreEqual(0, session._telemetry.BufferSize);
        }

        [Test]
        public void TestSelfDisablesOnSendException()
        {
            var requester = new MockTelemetryRestRequester { ThrowOnTelemetrySend = true };
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.SendBatch();

            Assert.IsFalse(session._telemetry.IsTelemetryEnabled());
        }

        [Test]
        public void TestDisabledTelemetryNoOps()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            // Simulate server disabling telemetry
            session.ParameterMap[SFSessionParameter.CLIENT_TELEMETRY_ENABLED] = "false";

            session._telemetry.AddLog(CreateTestTelemetryData());
            Assert.AreEqual(0, session._telemetry.BufferSize);
            Assert.AreEqual(0, requester.CapturedTelemetryRequests.Count);
        }

        [Test]
        public void TestAddLogAfterCloseDoesNotThrow()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.Close();

            Assert.DoesNotThrow(() => session._telemetry.AddLog(CreateTestTelemetryData()));
            Assert.AreEqual(0, session._telemetry.BufferSize);
        }

        [Test]
        public void TestSendBatchWithEmptyBufferSucceeds()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            var result = session._telemetry.SendBatch();

            Assert.IsTrue(result);
            Assert.AreEqual(0, requester.CapturedTelemetryRequests.Count);
        }

        [Test]
        public void TestThreadSafety()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);
            const int threadCount = 10;
            const int logsPerThread = 20;

            var threads = new List<Thread>();
            for (int t = 0; t < threadCount; t++)
            {
                var thread = new Thread(() =>
                {
                    for (int i = 0; i < logsPerThread; i++)
                    {
                        session._telemetry.AddLog(CreateTestTelemetryData());
                    }
                });
                threads.Add(thread);
            }

            threads.ForEach(t => t.Start());
            threads.ForEach(t => t.Join());

            // Wait for any background auto-flush to complete before manually sending remainder
            SpinWait.SpinUntil(() => session._telemetry.BufferSize < threadCount * logsPerThread, TimeSpan.FromSeconds(5));

            session._telemetry.SendBatch();

            var totalSent = requester.CapturedTelemetryRequests.Sum(r => r.Logs.Count);
            Assert.AreEqual(threadCount * logsPerThread, totalSent);
        }

        [Test]
        public async Task TestSendBatchAsync()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.AddLog(CreateTestTelemetryData());

            var result = await session._telemetry.SendBatchAsync();

            Assert.IsTrue(result);
            Assert.AreEqual(0, session._telemetry.BufferSize);
            Assert.AreEqual(1, requester.CapturedTelemetryRequests.Count);
            Assert.AreEqual(2, requester.CapturedTelemetryRequests.First().Logs.Count);
        }

        [Test]
        public async Task TestCloseAsync()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.AddLog(CreateTestTelemetryData());
            await session._telemetry.CloseAsync();

            Assert.IsTrue(session._telemetry.IsClosed);
            Assert.AreEqual(1, requester.CapturedTelemetryRequests.Count);
        }

        [Test]
        public void TestDoubleCloseDoesNotThrow()
        {
            var requester = new MockTelemetryRestRequester();
            var session = CreateSessionWithTelemetry(requester);

            session._telemetry.AddLog(CreateTestTelemetryData());
            session._telemetry.Close();

            Assert.DoesNotThrow(() => session._telemetry.Close());
            Assert.AreEqual(1, requester.CapturedTelemetryRequests.Count);
        }

        [Test]
        public void TestTelemetryEnabledByDefault()
        {
            var requester = new MockTelemetryRestRequester();
            var sessionContext = new SessionPropertiesContext { Password = null };
            var session = new SFSession(ConnectionString, sessionContext, requester);

            // Process login without CLIENT_TELEMETRY_ENABLED parameter
            session.ProcessLoginResponse(new LoginResponse
            {
                data = new LoginResponseData
                {
                    token = "session_token",
                    masterToken = "master_token",
                    authResponseSessionInfo = new SessionInfo(),
                    nameValueParameter = new List<NameValueParameter>()
                },
                success = true
            });

            Assert.IsNotNull(session._telemetry);
            Assert.IsTrue(session._telemetry.IsTelemetryEnabled());
        }
    }
}
