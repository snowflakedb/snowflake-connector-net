using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Moq;
using Newtonsoft.Json;
using NUnit.Framework;
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
        private const string AwsSignature = "3e46b07b306ff98878ca18aded8e79c55ddb6ddb99276f7d28bc299fe9e199ef"; // pragma: allowlist secret
        private static readonly string s_awsRequest = new StringBuilder()
            .Append("{\"method\":\"POST\",")
            .Append("\"url\":\"https://sts.eu-west-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15\",")
            .Append("\"headers\":{")
            .Append("\"Host\":\"sts.eu-west-1.amazonaws.com\",")
            .Append("\"X-Snowflake-Audience\":\"snowflakecomputing.com\",")
            .Append("\"x-amz-date\":\"20250527T142033Z\",")
            .Append($"\"x-amz-security-token\":\"{AwsToken}\",")
            .Append($"\"authorization\":\"AWS4-HMAC-SHA256 Credential={AwsAccessKey}/20250527/eu-west-1/sts/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token;x-snowflake-audience, Signature={AwsSignature}\"")
            .Append("}}")
            .ToString();
        private static readonly string s_awsRequestBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(s_awsRequest));
        internal static readonly DateTime s_utcNow = new(2025, 5, 27, 14, 20, 33, 11, new GregorianCalendar(), DateTimeKind.Utc);

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
            SetupSnowflakeAuthentication(_runner, AttestationProvider.AWS, s_awsRequestBase64);
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
            SetupSnowflakeAuthentication(_runner, AttestationProvider.AWS, s_awsRequestBase64);
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
            var signedJsonRequest = DecodeFromBase64(attestation.Credential);
            var signedRequest = JsonConvert.DeserializeObject<AttestationRequest>(signedJsonRequest);
            Assert.AreEqual("POST", signedRequest.Method);
            Assert.AreEqual($"https://sts.{AwsRegion}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15", signedRequest.Url);
            Assert.AreEqual($"sts.{AwsRegion}.amazonaws.com", signedRequest.Headers["Host"]);
            Assert.AreEqual("snowflakecomputing.com", signedRequest.Headers["X-Snowflake-Audience"]);
            Assert.AreEqual("20250527T142033Z", signedRequest.Headers["x-amz-date"]);
            Assert.AreEqual(AwsToken, signedRequest.Headers["x-amz-security-token"]);
            Assert.IsTrue(signedRequest.Headers["authorization"].StartsWith($"AWS4-HMAC-SHA256 Credential={AwsAccessKey}/20250527/{AwsRegion}/sts/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token;x-snowflake-audience, Signature="));
            Assert.AreEqual(64, ExtractSignature(signedRequest.Headers["authorization"]).Length);
            Assert.AreEqual(5, signedRequest.Headers.Count);
        }

        private string ExtractSignature(string authorizationHeader)
        {
            var signatureLabel = "Signature=";
            var position = authorizationHeader.IndexOf(signatureLabel);
            return authorizationHeader.Substring(position + signatureLabel.Length);
        }

        private string DecodeFromBase64(string base64String)
        {
            var bytes = Convert.FromBase64String(base64String);
            return Encoding.UTF8.GetString(bytes);
        }

        private SFSession PrepareSessionForAws(Action<Mock<EnvironmentOperations>> environmentOperationsConfigurator) =>
            PrepareSession(
                AttestationProvider.AWS,
                null,
                environmentOperationsConfigurator,
                t => SetupTime(t, s_utcNow),
                SetupAwsWrapper
            );

        internal static void SetupAwsWrapper(Mock<AwsSdkWrapper> awsSdkWrapper)
        {
            awsSdkWrapper
                .Setup(w => w.GetAwsRegion())
                .Returns(AwsRegion);
            awsSdkWrapper
                .Setup(w => w.GetAwsCredentials())
                .Returns(new ImmutableCredentials(AwsAccessKey, AwsSecretKey, AwsToken));
        }
    }
}
