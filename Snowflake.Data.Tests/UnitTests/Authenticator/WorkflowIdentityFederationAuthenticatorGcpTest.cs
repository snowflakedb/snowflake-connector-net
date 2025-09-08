using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture, NonParallelizable]
    public class WorkflowIdentityFederationAuthenticatorGcpTest : WorkloadIdentityFederationAuthenticatorTest
    {
        private static readonly string s_wifGcpMappingPath = Path.Combine(s_wifMappingPath, "GCP");
        private static readonly string s_wifGcpSuccessfulMappingPath = Path.Combine(s_wifGcpMappingPath, "successful_flow.json");
        private static readonly string s_wifGcpHttpErrorMappingPath = Path.Combine(s_wifGcpMappingPath, "http_error.json");
        internal const string JWTGCPToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE3NDM2OTIwMTcsImV4cCI6MTc3NTIyODAxNCwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoic29tZS1zdWJqZWN0In0.k7018udXQjw-sgVY8sTLTnNrnJoGwVpjE6HozZN-h0w"; // pragma: allowlist secret
        private const string JWTGCPTokenWithoutIssuer = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJzdWIiOiJzb21lLXN1YmplY3QiLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.H6sN6kjA82EuijFcv-yCJTqau5qvVTCsk0ZQ4gvFQMkB7c71XPs4lkwTa7ZlNNlx9e6TpN1CVGnpCIRDDAZaDw"; // pragma: allowlist secret
        private const string JWTGCPTokenWithoutSubject = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.w0njdpfWFETVK8Ktq9GdvuKRQJjvhOplcSyvQ_zHHwBUSMapqO1bjEWBx5VhGkdECZIGS1VY7db_IOqT45yOMA"; // pragma: allowlist secret
        private const string JWTGCPUnparsableToken = "unparsable.token";

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
        public void TestSuccessfulGCPAuthorization()
        {
            // arrange
            AddGcpWiremockMapping(JWTGCPToken);
            SetupSnowflakeAuthentication(_runner, AttestationProvider.GCP, JWTGCPToken);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestSuccessfulGCPAuthorizationAsync()
        {
            // arrange
            AddGcpWiremockMapping(JWTGCPToken);
            SetupSnowflakeAuthentication(_runner, AttestationProvider.GCP, JWTGCPToken);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestSuccessfulGCPAttestation()
        {
            // arrange
            AddGcpWiremockMapping(JWTGCPToken);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.GCP, attestation.Provider);
            Assert.AreEqual("some-subject", attestation.UserIdentifierComponents["sub"]);
            Assert.AreEqual(JWTGCPToken, attestation.Credential);
        }

        [Test]
        [TestCase(JWTGCPTokenWithoutIssuer, "Retrieving attestation for GCP failed. No issuer or subject found in the token.")]
        [TestCase(JWTGCPTokenWithoutSubject, "Retrieving attestation for GCP failed. No issuer or subject found in the token.")]
        [TestCase(JWTGCPUnparsableToken, "Retrieving attestation for GCP failed. Reading of the token failed.")]
        public void TestFailAttestationForInvalidToken(string token, string expectedErrorMessage)
        {
            // arrange
            AddGcpWiremockMapping(token);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain(expectedErrorMessage));
        }

        [Test]
        public void TestFailAttestationWhenHttpError()
        {
            // arrange
            _runner.AddMappings(s_wifGcpHttpErrorMappingPath);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain("Retrieving attestation for GCP failed. Failed to get token: Response status code does not indicate success: 400 (Bad Request)."));
        }

        private void AddGcpWiremockMapping(string token) =>
            AddGcpWiremockMapping(_runner, token);

        internal static void AddGcpWiremockMapping(WiremockRunner runner, string token) =>
            runner.AddMappings(s_wifGcpSuccessfulMappingPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, token));

        private SFSession PrepareSessionForGcp(string connectionStringSuffix,
            Action<Moq.Mock<EnvironmentOperations>> environmentOperationsConfigurator) =>
            PrepareSession(
                AttestationProvider.GCP,
                connectionStringSuffix,
                environmentOperationsConfigurator,
                SetupSystemTime,
                SetupAwsSdkDisabled
            );
    }
}
