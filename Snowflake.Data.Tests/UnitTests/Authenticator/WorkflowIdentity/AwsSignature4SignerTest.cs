using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using Amazon.Runtime;
using NUnit.Framework;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.WorkflowIdentity
{
    [TestFixture]
    public class AwsSignature4SignerTest
    {
        private const string AwsStsHost = "sts.eu-west-1.amazonaws.com";
        private const string AwsAccessKey = "ABCDEFGHIJ12345KLMNO"; // pragma: allowlist secret
        private const string AwsSecretKey = "aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStT"; // pragma: allowlist secret
        private const string AwsToken = "HIJKLMNOPQRSTUWXYZ"; // pragma: allowlist secret
        private const string SnowflakeAudience = "snowflakecomputing.com";
        private static readonly DateTime s_time = new(2025, 6, 12, 15, 46, 13, 5, new GregorianCalendar(), DateTimeKind.Utc);
        private const string ExpectedAmazonDate = "20250612T154613Z";
        private const string ExpectedSignature = "3fa477a5d4df0381fa0d303cc944723b20e6fff8e1917602a19f4dc67c18df17"; // pragma: allowlist secret
        private static readonly string s_expectedAuthorization = $"AWS4-HMAC-SHA256 Credential={AwsAccessKey}/20250612/eu-west-1/sts/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token;x-snowflake-audience, Signature={ExpectedSignature}";

        [Test]
        public void TestRequestSigning()
        {
            // arrange
            var request = new AttestationRequest
            {
                HttpMethod = HttpMethod.Post,
                Uri = new Uri($"https://{AwsStsHost}/?Action=GetCallerIdentity&Version=2011-06-15"),
                Headers = new Dictionary<string, string>
                {
                    { "host",  AwsStsHost },
                    { "x-snowflake-audience", SnowflakeAudience }
                }
            };
            var awsConfig = new AwsConfiguration()
            {
                Region = "eu-west-1",
                Service = "sts",
                Credentials = new ImmutableCredentials(AwsAccessKey, AwsSecretKey, AwsToken)
            };

            // act
            AwsSignature4Signer.AddTokenAndSignatureHeaders(request, awsConfig, s_time);

            // assert
            Assert.AreEqual(5, request.Headers.Count);
            Assert.AreEqual(AwsStsHost, request.Headers["host"]);
            Assert.AreEqual(SnowflakeAudience, request.Headers["x-snowflake-audience"]);
            Assert.AreEqual(ExpectedAmazonDate, request.Headers["x-amz-date"]);
            Assert.AreEqual(AwsToken, request.Headers["x-amz-security-token"]);
            Assert.AreEqual(s_expectedAuthorization, request.Headers["authorization"]);
        }
    }
}
