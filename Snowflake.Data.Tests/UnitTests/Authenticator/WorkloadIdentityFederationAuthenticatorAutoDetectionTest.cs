using System;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture, NonParallelizable]
    public class WorkloadIdentityFederationAuthenticatorAutoDetectionTest : WorkloadIdentityFederationAuthenticatorTest
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
        public void TestAutodetectOidcAttestation()
        {
            // arrange
            var token = WorkflowIdentityFederationAuthenticatorAzureTest.s_JWTAccessToken;
            var subject = WorkflowIdentityFederationAuthenticatorAzureTest.s_TokenSubject;
            var issuer = WorkflowIdentityFederationAuthenticatorAzureTest.s_TokenIssuer;
            var session = CreateSession($"token={token};", SetupAwsSdkDisabled);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.OIDC, attestation.Provider);
            Assert.AreEqual(token, attestation.Credential);
            Assert.AreEqual(subject, attestation.UserIdentifierComponents["sub"]);
            Assert.AreEqual(issuer, attestation.UserIdentifierComponents["iss"]);
        }

        [Test]
        public void TestAutodetectAzureAttestation()
        {
            // arrange
            WorkflowIdentityFederationAuthenticatorAzureTest.AddAzureBasicWiremockMappings(_runner);
            var session = CreateSession(null, SetupAwsSdkDisabled);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AZURE, attestation.Provider);
            Assert.IsFalse(string.IsNullOrEmpty(attestation.Credential));
        }

        [Test]
        public void TestAutodetectAwsAttestation()
        {
            // arrange
            var session = CreateSession(null, WorkloadIdentityFederationAuthenticatorAwsTest.SetupAwsWrapper);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AWS, attestation.Provider);
            Assert.IsFalse(string.IsNullOrEmpty(attestation.Credential));
        }

        [Test]
        public void TestAutodetectGcpAttestation()
        {
            // arrange
            WorkflowIdentityFederationAuthenticatorGcpTest.AddGcpWiremockMapping(_runner, WorkflowIdentityFederationAuthenticatorGcpTest.JWTGCPToken);
            var session = CreateSession(null, SetupAwsSdkDisabled);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.GCP, attestation.Provider);
            Assert.IsFalse(string.IsNullOrEmpty(attestation.Credential));
        }

        private SFSession CreateSession(string connectionStringSuffix, Action<Moq.Mock<AwsSdkWrapper>> awsSdkConfigurator) =>
            PrepareSession(
                null,
                connectionStringSuffix,
                SetupExperimentalAuthenticationEnabled,
                t => SetupTime(t, WorkloadIdentityFederationAuthenticatorAwsTest.s_utcNow),
                awsSdkConfigurator
            );
    }
}
