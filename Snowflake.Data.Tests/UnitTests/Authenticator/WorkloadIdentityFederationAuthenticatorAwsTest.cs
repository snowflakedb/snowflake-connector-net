using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public sealed class WorkloadIdentityFederationAuthenticatorAwsTest : WorkloadIdentityFederationAuthenticatorTest
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

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulAwsAuthorization()
        {
            // arrange
            SetupSnowflakeAuthentication(Runner, AttestationProvider.AWS, FakeJwt);
            var session = PrepareSessionForAws(NoEnvironmentSetup);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSuccessfulAwsAuthorizationAsync()
        {
            // arrange
            SetupSnowflakeAuthentication(Runner, AttestationProvider.AWS, FakeJwt);
            var session = PrepareSessionForAws(NoEnvironmentSetup);

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulAwsAttestation()
        {
            // arrange
            var session = PrepareSessionForAws(NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.Equal(AttestationProvider.AWS, attestation.Provider);
            Assert.Equal(FakeJwt, attestation.Credential);
            Assert.NotNull(attestation.UserIdentifierComponents);
            Assert.Empty(attestation.UserIdentifierComponents);
        }

        [SFFact]
        public void TestFailAttestationWhenStsCallFails()
        {
            // arrange - STS returns empty token
            Runner.AddMappings(s_wifAwsErrorStsPath);
            var session = PrepareSessionForAws(NoEnvironmentSetup, addStsMapping: false);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(authenticator.CreateAttestation);

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.Contains("Retrieving attestation for AWS failed.", thrown.Message);
        }

        [SFFact]
        public void TestFailAttestationWhenNoCredentials()
        {
            // arrange
            var session = PrepareSessionForAws(NoEnvironmentSetup, awsSdkConfigurator: SetupAwsWrapperNoCredentials);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(authenticator.CreateAttestation);

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.Contains("Retrieving attestation for AWS failed.", thrown.Message);
            Assert.Contains("Could not find AWS credentials", thrown.Message);
        }

        [SFFact]
        public void TestFailAttestationWhenNoRegion()
        {
            // arrange
            var session = PrepareSessionForAws(NoEnvironmentSetup, awsSdkConfigurator: SetupAwsWrapperNoRegion);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(authenticator.CreateAttestation);

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.Contains("Retrieving attestation for AWS failed.", thrown.Message);
            Assert.Contains("Could not find AWS region", thrown.Message);
        }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        public void TestSuccessfulAwsTransitiveImpersonation()
        {
            // arrange
            Runner.AddMappings(s_wifAwsImpersonationStsPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, FakeJwt));
            SetupSnowflakeAuthentication(Runner, AttestationProvider.AWS, FakeJwt);
            var session = PrepareSessionForAws(NoEnvironmentSetup, addStsMapping: false,
                connectionStringSuffix: "workload_impersonation_path=arn:aws:iam::123456789012:role/TestRole");

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact]
        public void TestSuccessfulAwsTransitiveImpersonationAttestation()
        {
            // arrange
            Runner.AddMappings(s_wifAwsImpersonationStsPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, FakeJwt));
            var session = PrepareSessionForAws(NoEnvironmentSetup, addStsMapping: false,
                connectionStringSuffix: "workload_impersonation_path=arn:aws:iam::123456789012:role/TestRole");
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.GetAuthenticator();

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.Equal(AttestationProvider.AWS, attestation.Provider);
            Assert.Equal(FakeJwt, attestation.Credential);
            Assert.NotNull(attestation.UserIdentifierComponents);
            Assert.Empty(attestation.UserIdentifierComponents);
        }

        private SFSession PrepareSessionForAws(
            Action<Mock<EnvironmentOperations>> environmentOperationsConfigurator,
            Action<Mock<AwsSdkWrapper>> awsSdkConfigurator = null,
            bool addStsMapping = true,
            string connectionStringSuffix = null)
        {
            if (addStsMapping)
                Runner.AddMappings(s_wifAwsSuccessfulStsPath, new StringTransformations().ThenTransform(s_accessTokenReplacement, FakeJwt));
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
