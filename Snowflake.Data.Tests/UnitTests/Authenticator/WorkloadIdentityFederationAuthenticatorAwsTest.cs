using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Moq;
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
    public class WorkloadIdentityFederationAuthenticatorAwsTest : WorkloadIdentityFederationAuthenticatorTest
    {
        private const string AwsRegion = "eu-west-1";
        private const string AwsAccessKey = "ABCDEFGHIJ12345KLMNO"; // pragma: allowlist secret
        private const string AwsSecretKey = "aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStT"; // pragma: allowlist secret
        private const string AwsToken = "HIJKLMNOPQRSTUWXYZ"; // pragma: allowlist secret
        private const string FakeJwt = "fake.jwt.fake-signature";
        private static readonly string s_wifAwsMappingPath = Path.Combine(s_wifMappingPath, "AWS");
        private static readonly string s_wifAwsSuccessfulStsPath = Path.Combine(s_wifAwsMappingPath, "successful_get_web_identity_token.json");
        private static readonly string s_wifAwsErrorStsPath = Path.Combine(s_wifAwsMappingPath, "error_get_web_identity_token.json");
        private static readonly string s_wifAwsImpersonationStsPath = Path.Combine(s_wifAwsMappingPath, "successful_impersonation.json");

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
        public void TestSuccessfulAwsAuthorization()
        {
            // arrange
            SetupSnowflakeAuthentication(_runner, AttestationProvider.AWS, FakeJwt);
            var session = PrepareSessionForAws(NoEnvironmentSetup);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestSuccessfulAwsAuthorizationAsync()
        {
            // arrange
            SetupSnowflakeAuthentication(_runner, AttestationProvider.AWS, FakeJwt);
            var session = PrepareSessionForAws(NoEnvironmentSetup);

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestSuccessfulAwsAttestation()
        {
            // arrange
            var session = PrepareSessionForAws(NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AWS, attestation.Provider);
            Assert.AreEqual(FakeJwt, attestation.Credential);
            Assert.IsNotNull(attestation.UserIdentifierComponents);
            Assert.AreEqual(0, attestation.UserIdentifierComponents.Count);
        }

        [Test]
        public void TestFailAttestationWhenStsCallFails()
        {
            // arrange - STS returns empty token
            _runner.AddMappings(s_wifAwsErrorStsPath);
            var session = PrepareSessionForAws(NoEnvironmentSetup, addStsMapping: false);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain("Retrieving attestation for AWS failed."));
            Assert.That(thrown.Message, Does.Contain("GetWebIdentityToken returned an empty token"));
        }

        [Test]
        public void TestFailAttestationWhenNoCredentials()
        {
            // arrange
            var session = PrepareSessionForAws(NoEnvironmentSetup, awsSdkConfigurator: SetupAwsWrapperNoCredentials);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain("Retrieving attestation for AWS failed."));
            Assert.That(thrown.Message, Does.Contain("Could not find AWS credentials"));
        }

        [Test]
        public void TestFailAttestationWhenNoRegion()
        {
            // arrange
            var session = PrepareSessionForAws(NoEnvironmentSetup, awsSdkConfigurator: SetupAwsWrapperNoRegion);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain("Retrieving attestation for AWS failed."));
            Assert.That(thrown.Message, Does.Contain("Could not find AWS region"));
        }

        [Test]
        public void TestSuccessfulAwsTransitiveImpersonation()
        {
            // arrange
            _runner.AddMappings(s_wifAwsImpersonationStsPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, FakeJwt));
            SetupSnowflakeAuthentication(_runner, AttestationProvider.AWS, FakeJwt);
            var session = PrepareSessionForAws(NoEnvironmentSetup, addStsMapping: false,
                connectionStringSuffix: "workload_impersonation_path=arn:aws:iam::123456789012:role/TestRole");

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestSuccessfulAwsTransitiveImpersonationAttestation()
        {
            // arrange
            _runner.AddMappings(s_wifAwsImpersonationStsPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, FakeJwt));
            var session = PrepareSessionForAws(NoEnvironmentSetup, addStsMapping: false,
                connectionStringSuffix: "workload_impersonation_path=arn:aws:iam::123456789012:role/TestRole");
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AWS, attestation.Provider);
            Assert.AreEqual(FakeJwt, attestation.Credential);
            Assert.AreEqual(0, attestation.UserIdentifierComponents.Count);
        }

        private SFSession PrepareSessionForAws(
            Action<Mock<EnvironmentOperations>> environmentOperationsConfigurator,
            Action<Mock<AwsSdkWrapper>> awsSdkConfigurator = null,
            bool addStsMapping = true,
            string connectionStringSuffix = null)
        {
            if (addStsMapping)
                _runner.AddMappings(s_wifAwsSuccessfulStsPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, FakeJwt));
            return PrepareSession(
                AttestationProvider.AWS,
                connectionStringSuffix,
                environmentOperationsConfigurator,
                t => SetupTime(t, DateTime.UtcNow),
                awsSdkConfigurator ?? SetupAwsWrapper
            );
        }

        private static void SetupAwsWrapper(Mock<AwsSdkWrapper> awsSdkWrapper)
        {
            awsSdkWrapper
                .Setup(w => w.GetAwsRegion())
                .Returns(AwsRegion);
            awsSdkWrapper
                .Setup(w => w.GetAwsCredentials())
                .Returns(new ImmutableCredentials(AwsAccessKey, AwsSecretKey, AwsToken));
        }

        private static void SetupAwsWrapperNoCredentials(Mock<AwsSdkWrapper> awsSdkWrapper)
        {
            awsSdkWrapper
                .Setup(w => w.GetAwsRegion())
                .Returns(AwsRegion);
            awsSdkWrapper
                .Setup(w => w.GetAwsCredentials())
                .Returns((ImmutableCredentials)null);
        }

        private static void SetupAwsWrapperNoRegion(Mock<AwsSdkWrapper> awsSdkWrapper)
        {
            awsSdkWrapper
                .Setup(w => w.GetAwsRegion())
                .Returns((string)null);
        }
    }
}
