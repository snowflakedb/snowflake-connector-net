using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    public sealed class RestRequesterWiremockTest
    {
        private static readonly string s_mappingPath = Path.Combine("wiremock", "RestRequester");
        private static readonly string s_loginMapping = Path.Combine(s_mappingPath, "login_success.json");
        private static readonly string s_queryTruncatedThenValid = Path.Combine(s_mappingPath, "query_truncated_then_valid.json");
        private static readonly string s_queryTruncatedAlways = Path.Combine(s_mappingPath, "query_truncated_always.json");

        private WiremockRunner _runner;

        [OneTimeSetUp]
        public void BeforeAll()
        {
            _runner = WiremockRunner.NewWiremock();
        }

        [SetUp]
        public void BeforeEach()
        {
            _runner.ResetMapping();
        }

        [OneTimeTearDown]
        public void AfterAll()
        {
            _runner.Stop();
        }

        [Test]
        public async Task TestExecuteReaderAsyncRetriesOnTruncatedJson()
        {
            // arrange
            _runner.AddMappings(s_loginMapping);
            _runner.AddMappings(s_queryTruncatedThenValid);

            using var conn = new SnowflakeDbConnection();
            conn.ConnectionString = BuildConnectionString();
            await conn.OpenAsync(CancellationToken.None);

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            // act
            using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None);

            // assert
            Assert.True(await reader.ReadAsync());
            Assert.Equal("1", reader.GetString(0));
        }

        [Test]
        public void TestExecuteReaderAsyncThrowsWhenAllRetriesFail()
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
            Assert.ThrowsAsync<JsonReaderException>(
                () => cmd.ExecuteReaderAsync(CancellationToken.None));
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
