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
    [CollectionDefinition(nameof(ProgrammaticAccessTokenAuthenticationTestFixture), DisableParallelization = true)]
    public sealed class ProgrammaticAccessTokenAuthenticationTestFixture : ICollectionFixture<ProgrammaticAccessTokenAuthenticationTestFixture.Fixture>
    {
        public sealed class Fixture : IDisposable
        {
            internal readonly WiremockRunner Runner;

            public Fixture()
            {
                Runner = WiremockRunner.NewWiremock();
            }

            public void Dispose()
            {
                Runner.Stop();
            }
        }
    }

    [Collection(nameof(ProgrammaticAccessTokenAuthenticationTestFixture))]
    public class ProgrammaticAccessTokenAuthenticationTest
    {
        private readonly ProgrammaticAccessTokenAuthenticationTestFixture.Fixture _fixture;
        private static readonly string s_patMappingPath = Path.Combine("wiremock", "PAT");
        private static readonly string s_successfulPatFlowMappingPath = Path.Combine(s_patMappingPath, "successful_flow.json");
        private static readonly string s_invalidPatFlowMappingPath = Path.Combine(s_patMappingPath, "invalid_pat_token.json");

        private const string MasterToken = "master token";
        private const string SessionToken = "session token";
        private const string SessionId = "1172562260498";
        private const string User = "MOCK_USERNAME";
        private const string Account = "MOCK_ACCOUNT_NAME";
        private const string Token = "MOCK_TOKEN";

        public ProgrammaticAccessTokenAuthenticationTest(ProgrammaticAccessTokenAuthenticationTestFixture.Fixture fixture)
        {
            _fixture = fixture;
            _fixture.Runner.ResetMapping();
        }

        [Fact]
        public void TestSuccessfulPatAuthentication()
        {
            // arrange
            _fixture.Runner.AddMappings(s_successfulPatFlowMappingPath);
            var session = PrepareSession();

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Fact]
        public async Task TestSuccessfulPatAuthenticationAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_successfulPatFlowMappingPath);
            var session = PrepareSession();

            // act
            await session.OpenAsync(CancellationToken.None);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Fact]
        public void TestInvalidPatAuthentication()
        {
            // arrange
            _fixture.Runner.AddMappings(s_invalidPatFlowMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.Contains("Programmatic access token is invalid.", thrown.Message);
        }

        [Fact]
        public async Task TestInvalidPatAuthenticationAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_invalidPatFlowMappingPath);
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
            var host = WiremockRunner.Host;
            var port = WiremockRunner.DefaultHttpPort;
            var scheme = "http";

            return new StringBuilder()
                .Append($"authenticator={authenticator};account={Account};user={User};")
                .Append($"db={db};role={role};warehouse={warehouse};host={host};port={port};scheme={scheme};")
                .Append($"token={Token}")
                .ToString();
        }

        private void AssertSessionSuccessfullyCreated(SFSession session)
        {
            Assert.Equal(SessionId, session.sessionId);
            Assert.Equal(MasterToken, session.masterToken);
            Assert.Equal(SessionToken, session.sessionToken);
        }
    }
}
