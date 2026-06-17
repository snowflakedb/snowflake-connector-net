using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;
using Snowflake.Data.Telemetry;
using Snowflake.Data.Tests.Util;
using Snowflake.Data.Tests.IntegrationTests;
using WireMock;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Telemetry
{
    /// <summary>
    /// End-to-(almost)-end telemetry tests using wiremock.
    /// Verifies the real pipeline: command execution -> activity -> SessionTelemetryModule -> POST /telemetry/send -> wiremock captures it.
    /// Assertions are made against what wiremock actually received, not against in-process activity captures.
    /// </summary>
    public sealed class ClientTelemetryWiremockTest
    {
        private static readonly string s_mappingPath = Path.Combine("wiremock", "Telemetry", "login_and_telemetry.json");
        private static readonly string s_telemetryDisabledMappingPath = Path.Combine("wiremock", "Telemetry", "login_telemetry_disabled.json");
        private static readonly string s_failingQueryMappingPath = Path.Combine("wiremock", "Telemetry", "failing_query.json");

        private readonly string _connectionString;
        private readonly string _connectionStringTelemetryDisabled;

        private static readonly HttpClient s_http = new();

        private readonly IWiremockRunner _wiremock;

        public ClientTelemetryWiremockTest()
        {
            _wiremock = WiremockRunner.NewWiremock();
            _wiremock.ResetMapping();
            _wiremock.AddMappings(s_mappingPath);

            var url = new Uri(_wiremock.WiremockBaseHttpUrl);
            _connectionString = $"account=testaccount;user=dummyuser;password=testpwd;host={url.Host};port={url.Port};scheme={url.Scheme};poolingEnabled=false;CLIENT_TELEMETRY_ENABLED=true;";
            _connectionStringTelemetryDisabled = $"account=testaccount;user=dummyuser;password=testpwd;host={url.Host};port={url.Port};scheme={url.Scheme};poolingEnabled=false;CLIENT_TELEMETRY_ENABLED=false;";
        }

        [SFTheory(SkipCondition.SkipOnJenkins)]
        [InlineData("ExecuteNonQuery")]
        [InlineData("ExecuteScalar")]
        [InlineData("ExecuteReader")]
        public async Task TestSyncCommandSendsTelemetryToServer(string method)
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            switch (method)
            {
                case "ExecuteNonQuery": cmd.ExecuteNonQuery(); break;
                case "ExecuteScalar": cmd.ExecuteScalar(); break;
                case "ExecuteReader": cmd.ExecuteReader().Dispose(); break;
            }

            await conn.CloseAsync();

            var logs = await GetTelemetryLogsAsync();
            var matching = logs.Where(l => l.Source == ActivityStarter.ActivitySourceName && l.StatusCode == "OK").ToList();
            Assert.NotEmpty(matching);
            Assert.Equal("client_activity", matching.First().Type);
        }

        [SFTheory(SkipCondition.SkipOnJenkins)]
        [InlineData("ExecuteNonQueryAsync")]
        [InlineData("ExecuteScalarAsync")]
        [InlineData("ExecuteReaderAsync")]
        public async Task TestAsyncCommandSendsTelemetryToServer(string method)
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            await conn.OpenAsync();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            switch (method)
            {
                case "ExecuteNonQueryAsync": await cmd.ExecuteNonQueryAsync(); break;
                case "ExecuteScalarAsync": await cmd.ExecuteScalarAsync(); break;
                case "ExecuteReaderAsync": (await cmd.ExecuteReaderAsync()).Dispose(); break;
            }

            await conn.CloseAsync(CancellationToken.None);

            var logs = await GetTelemetryLogsAsync();
            var matching = logs.Where(l => l.Source == ActivityStarter.ActivitySourceName && l.StatusCode == "OK").ToList();
            Assert.NotEmpty(matching);
            Assert.Equal("client_activity", matching.First().Type);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestTelemetryPayloadContainsSessionTags()
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.ExecuteNonQuery();

            await conn.CloseAsync();

            var logs = await GetTelemetryLogsAsync();
            Assert.NotEmpty(logs);

            var log = logs.First();
            Assert.Equal("snowflake", log.Tag(TelemetryTags.DbSystem));
            Assert.Equal("telemetry-test-session", log.Tag(TelemetryTags.SessionId));
            Assert.Equal("TEST_WH", log.Tag(TelemetryTags.DbWarehouse));
            Assert.Equal("TEST_ROLE", log.Tag(TelemetryTags.DbRole));
            Assert.Equal("TEST_DB", log.Tag(TelemetryTags.DbName));
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestTelemetryPayloadContainsDriverMetadata()
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.ExecuteNonQuery();

            await conn.CloseAsync();

            var logs = await GetTelemetryLogsAsync();
            Assert.NotEmpty(logs);

            var log = logs.First();
            Assert.Equal(".NET", log.DriverType);
            Assert.NotNull(log.DriverVersion);
            Assert.NotNull(log.Duration);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestServerOverridesClientTelemetrySetting()
        {
            // Client disables telemetry, but server responds with CLIENT_TELEMETRY_ENABLED=true
            // Server wins — telemetry should be sent
            using var conn = new SnowflakeDbConnection(_connectionStringTelemetryDisabled);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.ExecuteNonQuery();

            await conn.CloseAsync();

            var logs = await GetTelemetryLogsAsync();
            Assert.NotEmpty(logs); //"Server override should enable telemetry even when client disabled it"
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestServerDisabledTelemetrySendsNothingToServer()
        {
            _wiremock.AddMappings(s_telemetryDisabledMappingPath);

            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.ExecuteNonQuery();

            await conn.CloseAsync();

            var telemetryRequests = await GetWiremockRequestsBodiesToAsync("/telemetry/send", noRequestsExpected: true);
            Assert.Empty(telemetryRequests); // "Server override should enable telemetry even when client disabled it"
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestTelemetrySentWithCorrectAuthToken()
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.ExecuteNonQuery();

            await conn.CloseAsync();

            var telemetryRequests = await GetWiremockRequestsToAsync("/telemetry/send");
            Assert.NotEmpty(telemetryRequests);

            var authHeader = telemetryRequests.First().Headers["Authorization"]?.ToString();
            Assert.Equal("Snowflake Token=\"session-token\"", authHeader);
        }

        [SFFact(SkipCondition.SkipOnJenkins, RetriesCount = RetriesCount.Thrice)]
        public async Task TestCommandFailureSendsTelemetryWithErrorStatus()
        {
            _wiremock.AddMappings(s_failingQueryMappingPath);

            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            Assert.Throws<SnowflakeDbException>(() => cmd.ExecuteNonQuery());

            await conn.CloseAsync();

            var logs = await GetTelemetryLogsAsync();
            var errorLogs = logs.Where(l => l.StatusCode == "ERROR").ToList();
            Assert.NotEmpty(errorLogs); //"Expected at least one ERROR telemetry log when command fails"
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestPublicStartActivityWithSuccessDoesNotInterfereWithInternalTelemetry()
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();

            using (var activity = cmd.StartActivity("MyCustomOp"))
            {
                activity?.SetTag("app.module", "billing");
                activity?.SetTag("app.batch_size", "500");
                activity?.SetSuccess();
            }

            cmd.CommandText = "SELECT 1";
            cmd.ExecuteNonQuery();

            await conn.CloseAsync();

            var logs = await GetTelemetryLogsAsync();

            // Internal telemetry
            var internalLogs = logs.Where(l => l.Source == ActivityStarter.ActivitySourceName).ToList();
            Assert.NotEmpty(internalLogs); //"Internal telemetry should still be sent"
            Assert.Equal("OK", internalLogs.First().StatusCode);

            // Custom telemetry — single synthetic entry (no events added → display name becomes event name)
            var customLogs = logs.Where(l => l.Source == ActivityStarter.ClientDefinedTelemetrySourceName).ToList();
            AssertExtensions.Equal(1, customLogs.Count, "Exactly one custom telemetry log expected (MyCustomOp has no events, so one synthetic entry)");

            var custom = customLogs.Single();
            Assert.Equal("client_activity", custom.Type);
            Assert.Equal(ActivityStarter.ClientDefinedTelemetrySourceName, custom.Source);
            Assert.Equal("MyCustomOp", custom.EventName);
            Assert.Equal("OK", custom.StatusCode);
            Assert.Equal(".NET", custom.DriverType);
            Assert.Equal("billing", custom.Tag("app.module"));
            Assert.Equal("500", custom.Tag("app.batch_size"));
            Assert.Equal("telemetry-test-session", custom.Tag(TelemetryTags.SessionId));
            Assert.Equal("snowflake", custom.Tag(TelemetryTags.DbSystem));
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestPublicStartActivityWithExceptionDoesNotCrash()
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();

            using (var activity = cmd.StartActivity("FailingOp"))
            {
                activity?.SetException(new InvalidOperationException("something broke"));
            }

            cmd.CommandText = "SELECT 1";
            cmd.ExecuteNonQuery();

            conn.Close();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestPublicStartActivityWithTelemetryEvent()
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();

            // Capture custom activities to verify events
            var customActivities = new List<Activity>();
            using var customListener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == ActivityStarter.ClientDefinedTelemetrySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = customActivities.Add,
            };
            ActivitySource.AddActivityListener(customListener);

            using (var activity = cmd.StartActivity("MultiStepOp"))
            {
                activity?.AddTelemetryEvent("StepOneComplete");
                activity?.AddTelemetryEvent("StepTwoComplete");
                activity?.SetSuccess();
            }

            Assert.Equal(1, customActivities.Count);
            var captured = customActivities.Single();
            Assert.Equal("MultiStepOp", captured.OperationName);
            Assert.Equal(ActivityStarter.ClientDefinedTelemetrySourceName, captured.Source.Name);

            var eventNames = captured.Events.Select(e => e.Name).ToList();
            Assert.Contains("StepOneComplete", eventNames);
            Assert.Contains("StepTwoComplete", eventNames);

            conn.Close();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestPublicStartActivityThrowsWhenTelemetryDisabled()
        {
            _wiremock.ResetMapping();
            _wiremock.AddMappings(s_telemetryDisabledMappingPath);

            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();

            var ex = Assert.Throws<ArgumentException>(() => cmd.StartActivity("ShouldFail"));
            Assert.Contains("Client telemetry needs to be enabled", ex.Message);

            conn.Close();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestPublicStartActivityEnrichesWithSessionContext()
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();

            var customActivities = new List<Activity>();
            using var customListener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == ActivityStarter.ClientDefinedTelemetrySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = customActivities.Add,
            };
            ActivitySource.AddActivityListener(customListener);

            using (var activity = cmd.StartActivity("ContextCheck"))
            {
                activity?.SetSuccess();
            }

            Assert.Equal(1, customActivities.Count);
            var captured = customActivities.Single();
            Assert.Equal("snowflake", captured.GetTagItem(TelemetryTags.DbSystem));
            Assert.Equal("telemetry-test-session", captured.GetTagItem(TelemetryTags.SessionId));
            Assert.Equal("TEST_WH", captured.GetTagItem(TelemetryTags.DbWarehouse));
            Assert.Equal("TEST_ROLE", captured.GetTagItem(TelemetryTags.DbRole));
            Assert.Equal("TEST_DB", captured.GetTagItem(TelemetryTags.DbName));

            conn.Close();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestPublicStartActivityWithNestedActivitiesAndCommandExecution()
        {
            using var conn = new SnowflakeDbConnection(_connectionString);
            conn.Open();

            using var cmd = (SnowflakeDbCommand)conn.CreateCommand();

            using (var parentActvity = cmd.StartActivity("Parent"))
            {
                parentActvity?.AddTelemetryEvent("Parenting");

                using (var childActivity = cmd.StartActivity("OldestChild"))
                {
                    childActivity?.SetTag("some_sort_of_clients_psy_op", "1000");
                    childActivity?.AddTelemetryEvent("PsyOpDone");
                    childActivity?.SetSuccess();
                }

                using (var unsuccessfulChild = cmd.StartActivity("MiddleChild"))
                {
                    unsuccessfulChild.SetTag("Something", "123");
                    unsuccessfulChild.SetException(new AbandonedMutexException("Not great, not terrible"));
                }

                using (var youngestChild = cmd.StartActivity("YoungestChild"))
                {
                    youngestChild.SetCustomProperty("special", "✨");
                    youngestChild.SetBaggage("emotional", "one");
                }

                cmd.CommandText = "SELECT 1";
                cmd.ExecuteNonQuery();

                parentActvity?.AddTelemetryEvent("OperationComplete");
                parentActvity?.SetSuccess();
            }

            await conn.CloseAsync();

            var logs = await GetTelemetryLogsAsync();

            // Internal command telemetry
            var internalLogs = logs.Where(l => l.Source == ActivityStarter.ActivitySourceName).ToList();
            Assert.NotEmpty(internalLogs);
            Assert.Equal("OK", internalLogs.First().StatusCode);

            // Custom activities
            var customLogs = logs.Where(l => l.Source == ActivityStarter.ClientDefinedTelemetrySourceName).ToList();

            // Parent: synthetic event + 2 explicit events (Parenting, OperationComplete) → 3 log entries
            var parentSynthetic = customLogs.Single(l => l.EventName == "Parent");
            Assert.Equal("client_activity", parentSynthetic.Type);
            Assert.Equal("snowflake", parentSynthetic.Tag(TelemetryTags.DbSystem));
            Assert.Equal("telemetry-test-session", parentSynthetic.Tag(TelemetryTags.SessionId));
            var parentEventLogs = customLogs.Where(l => l.EventName is "Parenting" or "OperationComplete").ToList();
            AssertExtensions.Equal(2, parentEventLogs.Count, "Parent activity should produce 2 explicit event log entries");

            // OldestChild: synthetic event (with custom tag) + 1 explicit event (PsyOpDone)
            var oldestChildSynthetic = customLogs.Single(l => l.EventName == "OldestChild");
            Assert.Equal("OK", oldestChildSynthetic.StatusCode);
            Assert.Equal("1000", oldestChildSynthetic.Tag("some_sort_of_clients_psy_op"));

            // MiddleChild: synthetic event (with custom tag) + exception event → ERROR status
            var middleChildSynthetic = customLogs.Single(l => l.EventName == "MiddleChild");
            Assert.Equal("ERROR", middleChildSynthetic.StatusCode);
            Assert.Equal("123", middleChildSynthetic.Tag("Something"));

            // YoungestChild: no SetSuccess/SetException called → UNSET status, synthetic event with display name
            var youngestChildLog = customLogs.Single(l => l.EventName == "YoungestChild");
            Assert.Equal("UNSET", youngestChildLog.StatusCode);
        }

        #region Helpers

        private async Task<List<TelemetryLogEntry>> GetTelemetryLogsAsync()
        {
            var requests = await GetWiremockRequestsBodiesToAsync("/telemetry/send");
            var logs = new List<TelemetryLogEntry>();
            foreach (var req in requests)
            {
                if (req["logs"] is JArray logsArray)
                    logs.AddRange(logsArray.Select(t => new TelemetryLogEntry(t)));
            }
            return logs;
        }

        private async Task<List<IRequestMessage>> GetWiremockRequestsToAsync(string urlPath, int alreadyRetriedCount = 0, bool noRequestsExpected = false)
        {
            for (;;)
            {
                if (alreadyRetriedCount++ == 3) throw new TimeoutException("Wiremock returns no data!");

                var jsons = _wiremock
                    .GetCapturedRequests()
                    .Where(x => x.Path.Contains(urlPath))
                    .ToList();

                if (noRequestsExpected && alreadyRetriedCount == 3) return new List<IRequestMessage>();
                if (jsons.Count != 0) return jsons;

                await Task.Delay(500);
            }
        }

        private async Task<List<JToken>> GetWiremockRequestsBodiesToAsync(string urlPath, int alreadyRetriedCount = 0, bool noRequestsExpected = false)
        {
            var requests = await GetWiremockRequestsToAsync(urlPath, alreadyRetriedCount, noRequestsExpected);
            return requests.Where(x => x.Path.Contains(urlPath))
                .Where(x => x.Body != null)
                .Select(x => x.Body)
                .Select(JToken.Parse)
                .ToList();
        }

        /// <summary>
        /// Strongly typed view over a single telemetry log entry received by wiremock.
        /// Wraps the raw <c>{"message": {...}, "timestamp": ...}</c> JToken.
        /// </summary>
        private sealed class TelemetryLogEntry
        {
            private readonly JToken _message;

            internal TelemetryLogEntry(JToken raw) => _message = raw["message"];

            internal string Type => _message?[TelemetryField.Type]?.ToString();
            internal string Source => _message?[TelemetryField.Source]?.ToString();
            internal string EventName => _message?[TelemetryField.EventName]?.ToString();
            internal string StatusCode => _message?[TelemetryField.StatusCode]?.ToString();
            internal string DriverType => _message?[TelemetryField.DriverType]?.ToString();
            internal string DriverVersion => _message?[TelemetryField.DriverVersion]?.ToString();
            internal string Duration => _message?[TelemetryField.Duration]?.ToString();

            internal string Tag(string tagName) => _message?[$"tag.{tagName}"]?.ToString();
        }

        #endregion
    }
}
