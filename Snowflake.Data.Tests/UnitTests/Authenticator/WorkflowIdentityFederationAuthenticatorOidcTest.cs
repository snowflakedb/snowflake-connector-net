using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [CollectionDefinition(nameof(WorkflowIdentityFederationAuthenticatorOidcTestFixture), DisableParallelization = true)]
    public sealed class WorkflowIdentityFederationAuthenticatorOidcTestFixture : ICollectionFixture<WorkflowIdentityFederationAuthenticatorOidcTestFixture.Fixture>
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

    [Collection(nameof(WorkflowIdentityFederationAuthenticatorOidcTestFixture))]
    public sealed class WorkflowIdentityFederationAuthenticatorOidcTest : WorkloadIdentityFederationAuthenticatorTest
    {
        private readonly WorkflowIdentityFederationAuthenticatorOidcTestFixture.Fixture _fixture;

        public WorkflowIdentityFederationAuthenticatorOidcTest(WorkflowIdentityFederationAuthenticatorOidcTestFixture.Fixture fixture)
        {
            _fixture = fixture;
            _fixture.Runner.ResetMapping();
        }

        [Fact]
        public void TestSuccessfulOidcAuthentication()
        {
            // arrange
            var token = WorkflowIdentityFederationAuthenticatorAzureTest.s_JWTAccessToken;
            SetupSnowflakeAuthentication(_fixture.Runner, AttestationProvider.OIDC, token);
            var session = CreateSessionForOidc(token);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Fact]
        public async Task TestSuccessfulOidcAuthenticationAsync()
        {
            // arrange
            var token = WorkflowIdentityFederationAuthenticatorAzureTest.s_JWTAccessToken;
            SetupSnowflakeAuthentication(_fixture.Runner, AttestationProvider.OIDC, token);
            var session = CreateSessionForOidc(token);

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Fact]
        public void TestOidcAttestation()
        {
            // arrange
            var token = WorkflowIdentityFederationAuthenticatorAzureTest.s_JWTAccessToken;
            var subject = WorkflowIdentityFederationAuthenticatorAzureTest.s_TokenSubject;
            var issuer = WorkflowIdentityFederationAuthenticatorAzureTest.s_TokenIssuer;
            var session = CreateSessionForOidc(token);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.Equal(AttestationProvider.OIDC, attestation.Provider);
            Assert.Equal(subject, attestation.UserIdentifierComponents["sub"]);
            Assert.Equal(issuer, attestation.UserIdentifierComponents["iss"]);
            Assert.Equal(token, attestation.Credential);
        }

        [Fact]
        public void TestFailOidcAttestationForWrongToken()
        {
            // arrange
            var session = CreateSessionForOidc("unparsable.token");
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.Contains("Retrieving attestation for OIDC failed. Failed to parse a token for OIDC workload identity federation.", thrown.Message);
        }

        [Fact]
        public void TestFailOidcAttestationWhenImpersonationIsUsed()
        {
            // arrange
            var token = WorkflowIdentityFederationAuthenticatorAzureTest.s_JWTAccessToken;
            var session = PrepareSession(
                AttestationProvider.OIDC,
                $"token={token};workload_impersonation_path=some/impersonation/path;",
                NoEnvironmentSetup,
                SetupSystemTime,
                SetupAwsSdkDisabled
            );
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.Contains("Impersonation is not supported for OIDC workload identity provider", thrown.Message);
        }

        private SFSession CreateSessionForOidc(string token) =>
            PrepareSession(
                AttestationProvider.OIDC,
                $"token={token};",
                NoEnvironmentSetup,
                SetupSystemTime,
                SetupAwsSdkDisabled
            );
    }
}
