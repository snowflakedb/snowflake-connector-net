using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Newtonsoft.Json;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests
{
    [CollectionDefinition(nameof(RestRequesterWiremockTestFixture), DisableParallelization = true)]
    public sealed class RestRequesterWiremockTestFixture : ICollectionFixture<RestRequesterWiremockTestFixture>, IDisposable
    {
        internal IWiremockRunner Runner;

        public RestRequesterWiremockTestFixture()
        {
            if (SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnJenkins).ShouldSkip)
            {
                Runner = new Mock<IWiremockRunner>().Object;
                return;
            }

            Runner = WiremockRunner.NewWiremock();
        }

        public void Dispose()
        {
            Runner.Stop();
        }
    }

    [Collection(nameof(RestRequesterWiremockTestFixture))]
    public sealed class RestRequesterWiremockTest
    {
        private readonly IWiremockRunner _runner;
        private static readonly string s_mappingPath = Path.Combine("wiremock", "RestRequester");
        private static readonly string s_loginMapping = Path.Combine(s_mappingPath, "login_success.json");
        private static readonly string s_queryTruncatedThenValid = Path.Combine(s_mappingPath, "query_truncated_then_valid.json");
        private static readonly string s_queryTruncatedAlways = Path.Combine(s_mappingPath, "query_truncated_always.json");

        public RestRequesterWiremockTest(RestRequesterWiremockTestFixture fixture)
        {
            _runner = fixture.Runner;
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestExecuteReaderAsyncRetriesOnTruncatedJson()
        {
            // arrange
            _runner.AddMappings(s_loginMapping);
            _runner.AddMappings(s_queryTruncatedThenValid);

            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString = BuildConnectionString();
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            // act
            using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.True(await reader.ReadAsync().ConfigureAwait(false));
            Assert.Equal("1", reader.GetString(0));
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestExecuteReaderAsyncThrowsWhenAllRetriesFail()
        {
            // arrange
            _runner.AddMappings(s_loginMapping);
            _runner.AddMappings(s_queryTruncatedAlways);

            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString = BuildConnectionString();
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            // act & assert
            await Assert.ThrowsAsync<JsonReaderException>(
                () => cmd.ExecuteReaderAsync(CancellationToken.None)).ConfigureAwait(false);
        }

        private string BuildConnectionString()
        {
            var uri = new Uri(_runner.WiremockBaseHttpUrl);
            return new StringBuilder()
                .Append("account=")
                .Append("testaccount")
                .Append(";user=")
                .Append("test")
                .Append(";password=")
                .Append("test")
                .Append(";host=")
                .Append(uri.Host)
                .Append(";port=")
                .Append(uri.Port)
                .Append(";scheme=")
                .Append(uri.Scheme)
                .Append(";")
                .ToString();
        }
    }
}
