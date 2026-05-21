using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [CollectionDefinition(nameof(WorkflowIdentityFederationAuthenticatorGcpTestFixture), DisableParallelization = true)]
    public sealed class WorkflowIdentityFederationAuthenticatorGcpTestFixture : ICollectionFixture<WorkflowIdentityFederationAuthenticatorGcpTestFixture.Fixture>
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

    [Collection(nameof(WorkflowIdentityFederationAuthenticatorGcpTestFixture))]
    public class WorkflowIdentityFederationAuthenticatorGcpTest : WorkloadIdentityFederationAuthenticatorTest
    {
        private readonly WorkflowIdentityFederationAuthenticatorGcpTestFixture.Fixture _fixture;
        private static readonly string s_wifGcpMappingPath = Path.Combine(s_wifMappingPath, "GCP");
        private static readonly string s_wifGcpSuccessfulMappingPath = Path.Combine(s_wifGcpMappingPath, "successful_flow.json");
        private static readonly string s_wifGcpHttpErrorMappingPath = Path.Combine(s_wifGcpMappingPath, "http_error.json");
        internal const string JWTGCPToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE3NDM2OTIwMTcsImV4cCI6MTc3NTIyODAxNCwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoic29tZS1zdWJqZWN0In0.k7018udXQjw-sgVY8sTLTnNrnJoGwVpjE6HozZN-h0w"; // pragma: allowlist secret
        private const string JWTGCPTokenWithoutIssuer = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJzdWIiOiJzb21lLXN1YmplY3QiLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.H6sN6kjA82EuijFcv-yCJTqau5qvVTCsk0ZQ4gvFQMkB7c71XPs4lkwTa7ZlNNlx9e6TpN1CVGnpCIRDDAZaDw"; // pragma: allowlist secret
        private const string JWTGCPTokenWithoutSubject = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.w0njdpfWFETVK8Ktq9GdvuKRQJjvhOplcSyvQ_zHHwBUSMapqO1bjEWBx5VhGkdECZIGS1VY7db_IOqT45yOMA"; // pragma: allowlist secret
        private const string JWTGCPUnparsableToken = "unparsable.token";

        public WorkflowIdentityFederationAuthenticatorGcpTest(WorkflowIdentityFederationAuthenticatorGcpTestFixture.Fixture fixture)
        {
            _fixture = fixture;
            _fixture.Runner.ResetMapping();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulGCPAuthorization()
        {
            // arrange
            AddGcpWiremockMapping(JWTGCPToken);
            SetupSnowflakeAuthentication(_fixture.Runner, AttestationProvider.GCP, JWTGCPToken);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSuccessfulGCPAuthorizationAsync()
        {
            // arrange
            AddGcpWiremockMapping(JWTGCPToken);
            SetupSnowflakeAuthentication(_fixture.Runner, AttestationProvider.GCP, JWTGCPToken);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulGCPAttestation()
        {
            // arrange
            AddGcpWiremockMapping(JWTGCPToken);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.Equal(AttestationProvider.GCP, attestation.Provider);
            Assert.Equal("some-subject", attestation.UserIdentifierComponents["sub"]);
            Assert.Equal(JWTGCPToken, attestation.Credential);
        }

        [SFTheory]
        [InlineData(JWTGCPTokenWithoutIssuer, "Retrieving attestation for GCP failed. No issuer or subject found in the token.")]
        [InlineData(JWTGCPTokenWithoutSubject, "Retrieving attestation for GCP failed. No issuer or subject found in the token.")]
        [InlineData(JWTGCPUnparsableToken, "Retrieving attestation for GCP failed. Reading of the token failed.")]
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
            Assert.Contains(expectedErrorMessage, thrown.Message);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestFailAttestationWhenHttpError()
        {
            // arrange
            _fixture.Runner.AddMappings(s_wifGcpHttpErrorMappingPath);
            var session = PrepareSessionForGcp("", NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.Contains("Retrieving attestation for GCP failed. Failed to get token: Response status code does not indicate success: 400 (Bad Request).", thrown.Message);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulGCPTransitiveImpersonation()
        {
            // arrange
            _fixture.Runner.AddMappings(s_wifGcpTransitiveImpersonationMappingPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, JWTGCPToken));
            SetupSnowflakeAuthentication(_fixture.Runner, AttestationProvider.GCP, JWTGCPToken);
            var session = PrepareSessionForGcp($"workload_impersonation_path=target-sa@project.iam.gserviceaccount.com", NoEnvironmentSetup);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulGCPTransitiveImpersonationAttestation()
        {
            // arrange
            _fixture.Runner.AddMappings(s_wifGcpTransitiveImpersonationMappingPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, JWTGCPToken));
            var session = PrepareSessionForGcp($"workload_impersonation_path=target-sa@project.iam.gserviceaccount.com", NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.Equal(AttestationProvider.GCP, attestation.Provider);
            Assert.Equal("some-subject", attestation.UserIdentifierComponents["sub"]);
            Assert.Equal(JWTGCPToken, attestation.Credential);
        }

        private void AddGcpWiremockMapping(string token) =>
            AddGcpWiremockMapping(_fixture.Runner, token);

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

        private static readonly string s_wifGcpTransitiveImpersonationMappingPath = Path.Combine(s_wifGcpMappingPath, "successful_transitive_impersonation.json");
    }
}
