using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture, NonParallelizable]
    public class WorkflowIdentityFederationAuthenticatorOidcTest : WorkloadIdentityFederationAuthenticatorTest
    {
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
        public void TestSuccessfulOidcAuthentication()
        {
            // arrange
            var token = WorkflowIdentityFederationAuthenticatorAzureTest.s_JWTAccessToken;
            SetupSnowflakeAuthentication(_runner, AttestationProvider.OIDC, token);
            var session = CreateSessionForOidc(token);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestSuccessfulOidcAuthenticationAsync()
        {
            // arrange
            var token = WorkflowIdentityFederationAuthenticatorAzureTest.s_JWTAccessToken;
            SetupSnowflakeAuthentication(_runner, AttestationProvider.OIDC, token);
            var session = CreateSessionForOidc(token);

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
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
            Assert.AreEqual(AttestationProvider.OIDC, attestation.Provider);
            Assert.AreEqual(subject, attestation.UserIdentifierComponents["sub"]);
            Assert.AreEqual(issuer, attestation.UserIdentifierComponents["iss"]);
            Assert.AreEqual(token, attestation.Credential);
        }

        [Test]
        public void TestFailOidcAttestationForWrongToken()
        {
            // arrange
            var session = CreateSessionForOidc("unparsable.token");
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain("Retrieving attestation for OIDC failed. Failed to parse a token for OIDC workload identity federation."));
        }

        [Test]
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
            Assert.That(thrown.Message, Does.Contain("Impersonation is not supported for OIDC workload identity provider"));
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
