using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
    public class WorkflowIdentityFederationAuthenticatorAzureTest : WorkloadIdentityFederationAuthenticatorTest
    {
        private static readonly string s_wifAzureMappingPath = Path.Combine(s_wifMappingPath, "Azure");
        private static readonly string s_wifAzureBasicSuccessfulMapping = Path.Combine(s_wifAzureMappingPath, "successful_flow_basic.json");
        private static readonly string s_wifAzureBasicWithClientIdSuccessfulMapping = Path.Combine(s_wifAzureMappingPath, "successful_flow_basic_with_client_id.json");
        private static readonly string s_wifAzureFunctionsSuccessfulMappingPath = Path.Combine(s_wifAzureMappingPath, "successful_flow_azure_functions.json");
        private static readonly string s_wifAzureFunctionsNoClientIdSuccessfulMappingPath = Path.Combine(s_wifAzureMappingPath, "successful_flow_azure_functions_no_client_id.json");
        private static readonly string s_azureIdentityEndpoint = $"{s_wiremockUrl}/metadata/identity/endpoint/from/env";
        private static readonly string s_azureIdentityHeader = "some-identity-header-from-env";
        private static readonly string s_azureManagedClientId = "managed-client-id-from-env";
        private static readonly string s_customEntraResource = "api://1111111-2222-3333-44444-55555555";
        internal static readonly string s_JWTAccessToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6Ijk0ZGI4N2NiMjdmNjdjZDA1Zjk5OTlkZjMwNjg1NmQ4In0.eyJhdWQiOiJhcGkxIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvZmExNWQ2OTItZTljNy00NDYwLWE3NDMtMjlmMjk1MjIyMjkvIiwic3ViIjoiNzcyMTNFMzAtRThDQi00NTk1LUIxQjYtNUYwNTBFODMwOEZEIiwiZXhwIjoxNzQ0NzE2MDUxLCJpYXQiOjE3NDQ3MTI0NTEsImp0aSI6Ijg3MTMzNzcwMDk0MTZmYmFhNDM0MmFkMjMxZGUwMDBkIn0.C5jTYoybRs5YF5GvPgoDq4WK5U9-gDzh_N3IPaqEBI0IifdYSWpKQ72v3UISnVpp7Fc46C-ZC8kijUGe3IU9zA"; // pragma: allowlist secret
        internal static readonly string s_TokenSubject = "77213E30-E8CB-4595-B1B6-5F050E8308FD";
        internal static readonly string s_TokenIssuer = "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/";
        private static readonly string s_JWTAccessTokenV2 = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJhcGk6Ly9mZDNmNzUzYi1lZWQzLTQ2MmMtYjZhNy1hNGI1YmI2NTBhYWQiLCJleHAiOjE3NDQ3MTYwNTEsImlhdCI6MTc0NDcxMjQ1MSwiaXNzIjoiaHR0cHM6Ly9sb2dpbi5taWNyb3NvZnRvbmxpbmUuY29tL2ZhMTVkNjkyLWU5YzctNDQ2MC1hNzQzLTI5ZjI5NTIyMjI5LyIsImp0aSI6Ijg3MTMzNzcwMDk0MTZmYmFhNDM0MmFkMjMxZGUwMDBkIiwic3ViIjoiNzcyMTNFMzAtRThDQi00NTk1LUIxQjYtNUYwNTBFODMwOEZEIn0.5mAlEPkzHLR7YbllpKgk-8ZEd88XfzA15DUK8u1rLWs"; // pragma: allowlist secret
        private static readonly string s_TokenIssuerV2 = "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/";
        private static readonly string s_clientIdReplacement = "%CLIENT_ID%";
        private static readonly string s_entraResourceReplacement = "%ENTRA_RESOURCE%";
        private static readonly string s_identityHeaderReplacement = "%IDENTITY_HEADER%";

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
        public void TestSuccessfulAzureAuthorization()
        {
            // arrange
            AddAzureBasicWiremockMappings();
            SetupSnowflakeAuthentication(_runner, AttestationProvider.AZURE, s_JWTAccessToken);
            var session = PrepareSessionForAzure(null, NoEnvironmentSetup);

            // act
            session.Open();

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestSuccessfulAzureAuthorizationAsync()
        {
            // arrange
            AddAzureBasicWiremockMappings();
            SetupSnowflakeAuthentication(_runner, AttestationProvider.AZURE, s_JWTAccessToken);
            var session = PrepareSessionForAzure(null, NoEnvironmentSetup);

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestSuccessfulAzureAttestation()
        {
            // arrange
            AddAzureBasicWiremockMappings();
            var session = PrepareSessionForAzure(null, NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AZURE, attestation.Provider);
            Assert.AreEqual(s_TokenIssuer, attestation.UserIdentifierComponents["iss"]);
            Assert.AreEqual(s_TokenSubject, attestation.UserIdentifierComponents["sub"]);
            AssertExtensions.NotEmptyString(attestation.Credential);
        }

        [Test]
        public void TestSuccessfulAzureAttestationWithClientId()
        {
            // arrange
            AddAzureBasicWithClientIdWiremockMappings();
            var session = PrepareSessionForAzure(null, ConfigureIdentityClientId);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AZURE, attestation.Provider);
            Assert.AreEqual(s_TokenIssuer, attestation.UserIdentifierComponents["iss"]);
            Assert.AreEqual(s_TokenSubject, attestation.UserIdentifierComponents["sub"]);
            AssertExtensions.NotEmptyString(attestation.Credential);
        }

        [Test]
        public void TestSuccessfulAzureAttestationWithV2Issuer()
        {
            // arrange
            AddAzureBasicV2IssuerWiremockMappings();
            var session = PrepareSessionForAzure(null, NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AZURE, attestation.Provider);
            Assert.AreEqual(s_TokenIssuerV2, attestation.UserIdentifierComponents["iss"]);
            Assert.AreEqual(s_TokenSubject, attestation.UserIdentifierComponents["sub"]);
            AssertExtensions.NotEmptyString(attestation.Credential);
        }

        [Test]
        public void TestFailForUnparsableTokenAttestation()
        {
            AddAzureUnparsableTokenWiremockMappings();
            var session = PrepareSessionForAzure(null, NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain("Retrieving attestation for AZURE failed. Reading of the token failed."));
        }

        [Test]
        public void TestSuccessfulAzureFunctionsAttestations()
        {
            // arrange
            AddAzureFunctionsWiremockMappings();
            var session = PrepareSessionForAzure(null, e =>
            {
                ConfigureIdentityEndpoint(e);
                ConfigureIdentityHeader(e);
                ConfigureIdentityClientId(e);
            });
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AZURE, attestation.Provider);
            Assert.AreEqual(s_TokenIssuer, attestation.UserIdentifierComponents["iss"]);
            Assert.AreEqual(s_TokenSubject, attestation.UserIdentifierComponents["sub"]);
            AssertExtensions.NotEmptyString(attestation.Credential);
        }

        [Test]
        public void TestSuccessfulAzureFunctionsAttestationWithoutClientId()
        {
            // arrange
            AddAzureFunctionsWithoutClientIdWiremockMappings();
            var session = PrepareSessionForAzure(null, e =>
            {
                ConfigureIdentityEndpoint(e);
                ConfigureIdentityHeader(e);
            });
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AZURE, attestation.Provider);
            Assert.AreEqual(s_TokenIssuer, attestation.UserIdentifierComponents["iss"]);
            Assert.AreEqual(s_TokenSubject, attestation.UserIdentifierComponents["sub"]);
            AssertExtensions.NotEmptyString(attestation.Credential);
        }

        [Test]
        public void TestSuccessfulAzureFunctionsAttestationWithCustomEntraResource()
        {
            // arrange
            AddAzureFunctionsWithCustomEntraResourceWiremockMappings();
            var session = PrepareSessionForAzure($"workload_identity_entra_resource={s_customEntraResource};", e =>
            {
                ConfigureIdentityEndpoint(e);
                ConfigureIdentityHeader(e);
                ConfigureIdentityClientId(e);
            });
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AZURE, attestation.Provider);
            Assert.AreEqual(s_TokenIssuer, attestation.UserIdentifierComponents["iss"]);
            Assert.AreEqual(s_TokenSubject, attestation.UserIdentifierComponents["sub"]);
            AssertExtensions.NotEmptyString(attestation.Credential);
        }

        [Test]
        public void TestFailAzureFunctionsAttestationWithoutIdentityHeader()
        {
            // arrange
            AddAzureFunctionsWiremockMappings();
            var session = PrepareSessionForAzure(null, e =>
            {
                ConfigureIdentityEndpoint(e);
                ConfigureIdentityClientId(e);
            });
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain("Retrieving attestation for AZURE failed. Managed identity is not enabled on this Azure function."));
        }

        [Test]
        public void TestSuccessfulAzureFunctionsAttestationWithV2IssuerAuthentication()
        {
            // arrange
            AddAzureFunctionsWithV2IssuerWiremockMappings();
            var session = PrepareSessionForAzure(null, e =>
            {
                ConfigureIdentityEndpoint(e);
                ConfigureIdentityHeader(e);
                ConfigureIdentityClientId(e);
            });
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var attestation = authenticator.CreateAttestation();

            // assert
            Assert.AreEqual(AttestationProvider.AZURE, attestation.Provider);
            Assert.AreEqual(s_TokenIssuerV2, attestation.UserIdentifierComponents["iss"]);
            Assert.AreEqual(s_TokenSubject, attestation.UserIdentifierComponents["sub"]);
            AssertExtensions.NotEmptyString(attestation.Credential);
        }

        private void AddAzureBasicWiremockMappings() => AddAzureBasicWiremockMappings(_runner);

        internal static void AddAzureBasicWiremockMappings(WiremockRunner runner) =>
            runner.AddMappings(s_wifAzureBasicSuccessfulMapping,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, s_JWTAccessToken)
                    .ThenTransform(s_entraResourceReplacement, WorkflowIdentityAzureAttestationRetriever.DefaultWorkloadIdentityEntraResource)
            );

        private void AddAzureBasicV2IssuerWiremockMappings() =>
            _runner.AddMappings(s_wifAzureBasicSuccessfulMapping,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, s_JWTAccessTokenV2)
                    .ThenTransform(s_entraResourceReplacement, WorkflowIdentityAzureAttestationRetriever.DefaultWorkloadIdentityEntraResource)
            );

        private void AddAzureUnparsableTokenWiremockMappings() =>
            _runner.AddMappings(s_wifAzureBasicSuccessfulMapping,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, "unparsable.token")
                    .ThenTransform(s_entraResourceReplacement, WorkflowIdentityAzureAttestationRetriever.DefaultWorkloadIdentityEntraResource)
            );

        private void AddAzureFunctionsWiremockMappings() =>
            _runner.AddMappings(s_wifAzureFunctionsSuccessfulMappingPath,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, s_JWTAccessToken)
                    .ThenTransform(s_clientIdReplacement, s_azureManagedClientId)
                    .ThenTransform(s_entraResourceReplacement, WorkflowIdentityAzureAttestationRetriever.DefaultWorkloadIdentityEntraResource)
                    .ThenTransform(s_identityHeaderReplacement, s_azureIdentityHeader)
            );

        private void AddAzureFunctionsWithV2IssuerWiremockMappings() =>
            _runner.AddMappings(s_wifAzureFunctionsSuccessfulMappingPath,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, s_JWTAccessTokenV2)
                    .ThenTransform(s_clientIdReplacement, s_azureManagedClientId)
                    .ThenTransform(s_entraResourceReplacement, WorkflowIdentityAzureAttestationRetriever.DefaultWorkloadIdentityEntraResource)
                    .ThenTransform(s_identityHeaderReplacement, s_azureIdentityHeader)
            );

        private void AddAzureFunctionsWithCustomEntraResourceWiremockMappings() =>
            _runner.AddMappings(s_wifAzureFunctionsSuccessfulMappingPath,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, s_JWTAccessToken)
                    .ThenTransform(s_clientIdReplacement, s_azureManagedClientId)
                    .ThenTransform(s_entraResourceReplacement, s_customEntraResource)
                    .ThenTransform(s_identityHeaderReplacement, s_azureIdentityHeader)
            );

        private void AddAzureFunctionsWithoutClientIdWiremockMappings() =>
            _runner.AddMappings(s_wifAzureFunctionsNoClientIdSuccessfulMappingPath,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, s_JWTAccessToken)
                    .ThenTransform(s_entraResourceReplacement, WorkflowIdentityAzureAttestationRetriever.DefaultWorkloadIdentityEntraResource)
                    .ThenTransform(s_identityHeaderReplacement, s_azureIdentityHeader)
            );

        private void AddAzureBasicWithClientIdWiremockMappings() =>
            _runner.AddMappings(s_wifAzureBasicWithClientIdSuccessfulMapping,
                new StringTransformations()
                    .ThenTransform(s_accessTokenReplacement, s_JWTAccessToken)
                    .ThenTransform(s_clientIdReplacement, s_azureManagedClientId)
                    .ThenTransform(s_entraResourceReplacement, WorkflowIdentityAzureAttestationRetriever.DefaultWorkloadIdentityEntraResource)
            );

        [Test]
        public void TestFailAzureAttestationWhenImpersonationIsUsed()
        {
            // arrange
            var session = PrepareSessionForAzure("workload_impersonation_path=some/impersonation/path;", NoEnvironmentSetup);
            var authenticator = (WorkloadIdentityFederationAuthenticator)session.authenticator;

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => authenticator.CreateAttestation());

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(thrown, SFError.WIF_ATTESTATION_ERROR);
            Assert.That(thrown.Message, Does.Contain("Impersonation is not supported for Azure workload identity provider"));
        }

        private SFSession PrepareSessionForAzure(string connectionStringSuffix,
            Action<Mock<EnvironmentOperations>> environmentOperationsConfigurator) =>
            PrepareSession(
                AttestationProvider.AZURE,
                connectionStringSuffix,
                environmentOperationsConfigurator,
                SetupSystemTime,
                SetupAwsSdkDisabled);

        private void ConfigureIdentityEndpoint(Mock<EnvironmentOperations> environmentOperations)
        {
            environmentOperations
                .Setup(e => e.GetEnvironmentVariable("IDENTITY_ENDPOINT"))
                .Returns(s_azureIdentityEndpoint);
        }

        private void ConfigureIdentityHeader(Mock<EnvironmentOperations> environmentOperations)
        {
            environmentOperations
                .Setup(e => e.GetEnvironmentVariable("IDENTITY_HEADER"))
                .Returns(s_azureIdentityHeader);
        }

        private void ConfigureIdentityClientId(Mock<EnvironmentOperations> environmentOperations)
        {
            environmentOperations
                .Setup(e => e.GetEnvironmentVariable("MANAGED_IDENTITY_CLIENT_ID"))
                .Returns(s_azureManagedClientId);
        }
    }
}
