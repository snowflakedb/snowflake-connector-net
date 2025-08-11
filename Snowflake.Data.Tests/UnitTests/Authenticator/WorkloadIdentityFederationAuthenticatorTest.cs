using System;
using System.IO;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class WorkloadIdentityFederationAuthenticatorTest
    {
        protected static readonly string s_wiremockUrl = $"http://localhost:{WiremockRunner.DefaultHttpPort}";
        protected static readonly string s_wifMappingPath = Path.Combine("wiremock", "WIF");
        protected static readonly string s_accessTokenReplacement = "%ACCESS_TOKEN%";
        protected static readonly string s_WifProviderReplacement = "%WIF_PROVIDER%";
        private const string MasterToken = "masterToken123";
        private const string SessionToken = "sessionToken123";
        private const string SessionId = "1234567890";
        private static readonly string s_SuccessfulMappingPath = Path.Combine(s_wifMappingPath, "snowflake_successful_login.json");

        internal SFSession PrepareSession(
            AttestationProvider? attestationProvider,
            string connectionStringSuffix,
            Action<Mock<EnvironmentOperations>> environmentOperationsConfigurator,
            Action<Mock<TimeProvider>> timeProviderConfigurator,
            Action<Mock<AwsSdkWrapper>> awsSdkConfigurator)
        {
            var wifProviderPart = attestationProvider == null ? string.Empty : $"workload_identity_provider={attestationProvider.ToString()};";
            var connectionString = $"authenticator=workload_identity;account=testaccount;{wifProviderPart}{connectionStringSuffix ?? string.Empty};host=localhost;port={WiremockRunner.DefaultHttpPort};scheme=http;";
            var sessionContext = new SessionPropertiesContext();
            var session = new SFSession(connectionString, sessionContext);
            var environmentOperations = new Mock<EnvironmentOperations>();
            environmentOperationsConfigurator(environmentOperations);
            var timeProvider = new Mock<TimeProvider>();
            timeProviderConfigurator(timeProvider);
            var awsSdkWrapper = new Mock<AwsSdkWrapper>();
            awsSdkConfigurator(awsSdkWrapper);
            var authenticator = new WorkloadIdentityFederationAuthenticator(session, environmentOperations.Object, timeProvider.Object, awsSdkWrapper.Object, s_wiremockUrl);
            session.ReplaceAuthenticator(authenticator);
            return session;
        }

        [Test]
        public void TestFailsAuthorizationWhenProviderIsNotGiven()
        {
            // arrange/act
            var exception = Assert.Throws<SnowflakeDbException>(() => PrepareSession(null, null, NoEnvironmentSetup, SetupSystemTime, SetupAwsSdkDisabled));

            // assert
            Assert.That(exception?.Message, Does.Contain("Required property WORKLOAD_IDENTITY_PROVIDER is not provided"));
        }

        [Test]
        public void TestFailsWithWifProviderExceptionMessageAttachedToSnowflakeException()
        {
            // arrange: throws exception with "Not available" message
            var session = PrepareSession(AttestationProvider.AWS, null, NoEnvironmentSetup, SetupSystemTime, SetupAwsSdkDisabled);

            // act
            var exception = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.That(exception?.Message, Does.Contain("Retrieving attestation for AWS failed. Not available"));
        }

        internal void NoEnvironmentSetup(Mock<EnvironmentOperations> environmentOperations)
        {
        }

        internal void SetupSystemTime(Mock<TimeProvider> timeProvider) =>
            timeProvider
                .Setup(t => t.UtcNow())
                .Returns(() => DateTime.UtcNow);

        internal void SetupTime(Mock<TimeProvider> timeProvider, DateTime utcNow) =>
            timeProvider
                .Setup(t => t.UtcNow())
                .Returns(utcNow);

        internal void SetupAwsSdkDisabled(Mock<AwsSdkWrapper> awsSdkWrapper)
        {
            awsSdkWrapper
                .Setup(a => a.GetAwsCredentials())
                .Throws(() => new Exception("Not available"));

            awsSdkWrapper
                .Setup(a => a.GetAwsRegion())
                .Throws(() => new Exception("Not available"));
        }

        internal void SetupSnowflakeAuthentication(WiremockRunner runner, AttestationProvider provider, string accessToken) =>
            runner.AddMappings(s_SuccessfulMappingPath,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, accessToken)
                    .ThenTransform(s_WifProviderReplacement, provider.ToString())
            );

        protected void AssertSessionSuccessfullyCreated(SFSession session)
        {
            Assert.AreEqual(SessionId, session.sessionId);
            Assert.AreEqual(MasterToken, session.masterToken);
            Assert.AreEqual(SessionToken, session.sessionToken);
        }
    }
}
