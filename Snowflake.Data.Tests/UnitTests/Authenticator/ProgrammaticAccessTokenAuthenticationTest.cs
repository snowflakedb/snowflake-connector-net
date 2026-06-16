using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public sealed class ProgrammaticAccessTokenAuthenticationTest : IDisposable
    {
        private readonly IWiremockRunner _runner;
        private static readonly string s_patMappingPath = Path.Combine("wiremock", "PAT");
        private static readonly string s_successfulPatFlowMappingPath = Path.Combine(s_patMappingPath, "successful_flow.json");
        private static readonly string s_invalidPatFlowMappingPath = Path.Combine(s_patMappingPath, "invalid_pat_token.json");

        private const string MasterToken = "master token";
        private const string SessionToken = "session token";
        private const string SessionId = "1172562260498";
        private const string User = "MOCK_USERNAME";
        private const string Account = "MOCK_ACCOUNT_NAME";
        private const string Token = "MOCK_TOKEN";

        public ProgrammaticAccessTokenAuthenticationTest()
        {
            _runner = WiremockRunner.NewWiremock();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulPatAuthentication()
        {
            // arrange
            _runner.AddMappings(s_successfulPatFlowMappingPath);
            var session = PrepareSession();

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSuccessfulPatAuthenticationAsync()
        {
            // arrange
            _runner.AddMappings(s_successfulPatFlowMappingPath);
            var session = PrepareSession();

            // act
            await session.OpenAsync(CancellationToken.None);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestInvalidPatAuthentication()
        {
            // arrange
            _runner.AddMappings(s_invalidPatFlowMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.Contains("Programmatic access token is invalid.", thrown.Message);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestInvalidPatAuthenticationAsync()
        {
            // arrange
            _runner.AddMappings(s_invalidPatFlowMappingPath);
            var session = PrepareSession();

            // act
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None));

            // assert
            Assert.Contains("Programmatic access token is invalid.", thrown.Message);
        }

        private SFSession PrepareSession()
        {
            var connectionString = GetPatConnectionString();
            var sessionContext = new SessionPropertiesContext();
            return new SFSession(connectionString, sessionContext);
        }

        private string GetPatConnectionString()
        {
            var authenticator = ProgrammaticAccessTokenAuthenticator.AuthName;
            var db = "testDb";
            var role = "ANALYST";
            var warehouse = "testWarehouse";
            var uri = new Uri(_runner.WiremockBaseHttpUrl);

            return new StringBuilder()
                .Append($"authenticator={authenticator};account={Account};user={User};")
                .Append($"db={db};role={role};warehouse={warehouse};host={uri.Host};port={uri.Port};scheme={uri.Scheme};")
                .Append($"token={Token}")
                .ToString();
        }

        private void AssertSessionSuccessfullyCreated(SFSession session)
        {
            Assert.Equal(SessionId, session.sessionId);
            Assert.Equal(MasterToken, session.masterToken);
            Assert.Equal(SessionToken, session.sessionToken);
        }

        public void Dispose()
        {
            _runner?.Dispose();
        }
    }
}
