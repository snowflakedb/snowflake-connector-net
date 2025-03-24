using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture, NonParallelizable]
    public class ProgrammaticAccessTokenAuthenticationTest
    {
        private static readonly string s_patMappingPath = Path.Combine("wiremock", "PAT");
        private static readonly string s_successfulPatFlowMappingPath = Path.Combine(s_patMappingPath, "successful_flow.json");
        private static readonly string s_invalidPatFlowMappingPath = Path.Combine(s_patMappingPath, "invalid_pat_token.json");

        private const string MasterToken = "master token";
        private const string SessionToken = "session token";
        private const string SessionId = "1172562260498";
        private const string User = "MOCK_USERNAME";
        private const string Account = "MOCK_ACCOUNT_NAME";
        private const string Token = "MOCK_TOKEN";

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
        public void TestSuccessfulPatAuthentication([Values] bool provideTokenByPassword)
        {
            // arrange
            _runner.AddMappings(s_successfulPatFlowMappingPath);
            var session = PrepareSession(provideTokenByPassword);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestSuccessfulPatAuthenticationAsync([Values] bool provideTokenByPassword)
        {
            // arrange
            _runner.AddMappings(s_successfulPatFlowMappingPath);
            var session = PrepareSession(provideTokenByPassword);

            // act
            await session.OpenAsync(CancellationToken.None);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestInvalidPatAuthentication()
        {
            // arrange
            _runner.AddMappings(s_invalidPatFlowMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.That(thrown.Message, Contains.Substring("Programmatic access token is invalid."));
        }

        [Test]
        public void TestInvalidPatAuthenticationAsync()
        {
            // arrange
            _runner.AddMappings(s_invalidPatFlowMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None));

            // assert
            Assert.That(thrown.Message, Contains.Substring("Programmatic access token is invalid."));
        }

        private SFSession PrepareSession(bool provideTokenByPassword = false)
        {
            var connectionString = GetPatConnectionString(provideTokenByPassword);
            var sessionContext = new SessionPropertiesContext();
            return new SFSession(connectionString, sessionContext);
        }

        private string GetPatConnectionString(bool provideTokenByPassword)
        {
            var authenticator = ProgrammaticAccessTokenAuthenticator.AuthName;
            var db = "testDb";
            var role = "ANALYST";
            var warehouse = "testWarehouse";
            var host = WiremockRunner.Host;
            var port = WiremockRunner.DefaultHttpPort;
            var scheme = "http";

            var connectionStringBuilder = new StringBuilder()
                .Append($"authenticator={authenticator};account={Account};user={User};")
                .Append($"db={db};role={role};warehouse={warehouse};host={host};port={port};scheme={scheme};");
            if (provideTokenByPassword)
            {
                connectionStringBuilder.Append($"password={Token}");
            }
            else
            {
                connectionStringBuilder.Append($"token={Token}");
            }
            return connectionStringBuilder.ToString();
        }

        private void AssertSessionSuccessfullyCreated(SFSession session)
        {
            Assert.AreEqual(SessionId, session.sessionId);
            Assert.AreEqual(MasterToken, session.masterToken);
            Assert.AreEqual(SessionToken, session.sessionToken);
        }
    }
}
