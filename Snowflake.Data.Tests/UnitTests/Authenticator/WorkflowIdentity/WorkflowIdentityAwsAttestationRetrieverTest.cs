using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using Amazon.Runtime;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Core.Rest;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.WorkflowIdentity
{
    [TestFixture]
    public class WorkflowIdentityAwsAttestationRetrieverTest
    {
        private const string ValidGetIdentityTokenResponseXml =
            @"<GetWebIdentityTokenResponse xmlns=""https://sts.amazonaws.com/doc/2011-06-15/"">
              <GetWebIdentityTokenResult>
                <WebIdentityToken>fake.jwt.token</WebIdentityToken>
                <Expiration>2099-01-01T00:00:00.000Z</Expiration>
              </GetWebIdentityTokenResult>
            </GetWebIdentityTokenResponse>";

        private const string EmptyGetIdentityTokenResponseXml =
            @"<GetWebIdentityTokenResponse xmlns=""https://sts.amazonaws.com/doc/2011-06-15/"">
              <GetWebIdentityTokenResult>
                <WebIdentityToken></WebIdentityToken>
                <Expiration>2099-01-01T00:00:00.000Z</Expiration>
              </GetWebIdentityTokenResult>
            </GetWebIdentityTokenResponse>";

        [Test]
        [TestCase("us-east-1", "sts.us-east-1.amazonaws.com")]
        [TestCase("cn-northwest-1", "sts.cn-northwest-1.amazonaws.com.cn")]
        public void TestBuildStsRequest(string region, string expectedHost)
        {
            // arrange
            var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
            var utcNow = new DateTime(2025, 6, 1, 10, 30, 0, DateTimeKind.Utc);
            var retriever = CreateRetriever(restRequester.Object, stsHost: null, utcNow: utcNow);
            var credentials = new ImmutableCredentials("akid", "secret", "session-token");
            var queryParams = "Action=GetWebIdentityToken&Version=2011-06-15&Audience.member.1=snowflakecomputing.com&SigningAlgorithm=ES384";

            // act
            var request = retriever.BuildStsRequest(region, queryParams, credentials);

            // assert
            var url = request.RequestUri.ToString();
            Assert.That(url, Does.StartWith($"https://{expectedHost}/?Action=GetWebIdentityToken&Version=2011-06-15"));
            Assert.That(url, Does.Contain("Audience.member.1=snowflakecomputing.com"));
            Assert.That(url, Does.Contain("SigningAlgorithm=ES384"));
            Assert.AreEqual(HttpMethod.Post, request.Method);
            Assert.AreEqual(expectedHost, request.Headers.GetValues("Host").First());
            Assert.AreEqual("20250601T103000Z", request.Headers.GetValues("x-amz-date").First());
            Assert.AreEqual("session-token", request.Headers.GetValues("x-amz-security-token").First());
        }

        [Test]
        public void TestGetWebIdentityTokenReturnsJwt()
        {
            // arrange
            var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
            var retriever = CreateRetriever(restRequester.Object);

            // act
            var credentials = new ImmutableCredentials("akid", "secret", "session-token");
            var jwt = retriever.GetWebIdentityToken("us-east-1", credentials);

            // assert
            Assert.AreEqual("fake.jwt.token", jwt);
        }

        [Test]
        public void TestGetWebIdentityTokenThrowsWhenTokenIsEmpty()
        {
            // arrange
            var restRequester = CreateMockRestRequester(EmptyGetIdentityTokenResponseXml);
            var retriever = CreateRetriever(restRequester.Object);

            // act/assert
            var credentials = new ImmutableCredentials("akid", "secret", "session-token");
            var exception = Assert.Throws<SnowflakeDbException>(() => retriever.GetWebIdentityToken("us-east-1", credentials));
            Assert.That(exception.Message, Does.Contain("GetWebIdentityToken returned an empty token"));
        }

        [Test]
        public void TestGetWebIdentityTokenThrowsWhenStsCallFails()
        {
            // arrange
            var restRequester = new Mock<IRestRequester>();
            restRequester
                .Setup(r => r.Get(It.IsAny<IRestRequest>()))
                .Throws(new HttpRequestException("connection refused"));
            var retriever = CreateRetriever(restRequester.Object);

            // act/assert
            var credentials = new ImmutableCredentials("akid", "secret", "session-token");
            var exception = Assert.Throws<SnowflakeDbException>(() => retriever.GetWebIdentityToken("us-east-1", credentials));
            Assert.That(exception.Message, Does.Contain("Failed to call AWS STS GetWebIdentityToken"));
            Assert.That(exception.Message, Does.Contain("connection refused"));
        }

        [Test]
        public void TestCreateAttestationDataReturnsJwtCredential()
        {
            // arrange
            var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
            var awsSdkWrapper = new Mock<AwsSdkWrapper>();
            awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns("us-east-1");
            awsSdkWrapper.Setup(w => w.GetAwsCredentials()).Returns(new ImmutableCredentials("akid", "secret", "token"));
            var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

            // act
            var attestation = retriever.CreateAttestationData(null, null);

            // assert
            Assert.AreEqual(AttestationProvider.AWS, attestation.Provider);
            Assert.AreEqual("fake.jwt.token", attestation.Credential);
            Assert.IsNotNull(attestation.UserIdentifierComponents);
            Assert.AreEqual(0, attestation.UserIdentifierComponents.Count);
        }

        [Test]
        public void TestCreateAttestationDataThrowsWhenNoCredentials()
        {
            // arrange
            var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
            var awsSdkWrapper = new Mock<AwsSdkWrapper>();
            awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns("us-east-1");
            awsSdkWrapper.Setup(w => w.GetAwsCredentials()).Returns((ImmutableCredentials)null);
            var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

            // act/assert
            var exception = Assert.Throws<SnowflakeDbException>(() => retriever.CreateAttestationData(null, null));
            Assert.That(exception.Message, Does.Contain("Could not find AWS credentials"));
        }

        [Test]
        public void TestCreateAttestationDataThrowsWhenNoRegion()
        {
            // arrange
            var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
            var awsSdkWrapper = new Mock<AwsSdkWrapper>();
            awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns((string)null);
            var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

            // act/assert
            var exception = Assert.Throws<SnowflakeDbException>(() => retriever.CreateAttestationData(null, null));
            Assert.That(exception.Message, Does.Contain("Could not find AWS region"));
        }

        [Test]
        public void TestCreateAttestationDataWithEmptyImpersonationPathUsesDirectCredentials()
        {
            // arrange
            var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
            var awsSdkWrapper = new Mock<AwsSdkWrapper>();
            awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns("us-east-1");
            awsSdkWrapper.Setup(w => w.GetAwsCredentials()).Returns(new ImmutableCredentials("akid", "secret", "token"));
            var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

            // act
            var attestation = retriever.CreateAttestationData(null, null, impersonationPath: "");

            // assert
            Assert.AreEqual("fake.jwt.token", attestation.Credential);
        }

        [Test]
        public void TestCreateAttestationDataWithSingleRoleImpersonation()
        {
            // arrange
            const string AssumeRoleResponseXml =
                @"<AssumeRoleResponse xmlns=""https://sts.amazonaws.com/doc/2011-06-15/"">
                  <AssumeRoleResult>
                    <Credentials>
                      <AccessKeyId>assumed-akid</AccessKeyId>
                      <SecretAccessKey>assumed-key</SecretAccessKey>
                      <SessionToken>assumed-token</SessionToken>
                    </Credentials>
                  </AssumeRoleResult>
                </AssumeRoleResponse>";

            var capturedUrls = new List<string>();
            var restRequester = new Mock<IRestRequester>();
            restRequester
                .Setup(r => r.Get(It.IsAny<IRestRequest>()))
                .Returns<IRestRequest>(r =>
                {
                    var url = ((RestRequestWrapper)r).ToRequestMessage(HttpMethod.Post).RequestUri.ToString();
                    capturedUrls.Add(url);
                    var body = url.Contains("AssumeRole") ? AssumeRoleResponseXml : ValidGetIdentityTokenResponseXml;
                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) };
                });

            var awsSdkWrapper = new Mock<AwsSdkWrapper>();
            awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns("us-east-1");
            awsSdkWrapper.Setup(w => w.GetAwsCredentials()).Returns(new ImmutableCredentials("initial-akid", "initial-secret", "initial-token"));
            var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

            // act
            var attestation = retriever.CreateAttestationData(null, null, impersonationPath: "arn:aws:iam::123456789012:role/TestRole");

            // assert
            Func<string, string> expectedRoleTransform =
#if NET462 || NET471
            x => x;
#else
                Uri.EscapeDataString;
#endif

            Assert.AreEqual("fake.jwt.token", attestation.Credential);
            Assert.AreEqual(2, capturedUrls.Count);
            Assert.That(capturedUrls[0], Does.Contain("Action=AssumeRole"));
            Assert.That(capturedUrls[0], Does.Contain(expectedRoleTransform("arn:aws:iam::123456789012:role/TestRole")));
            Assert.That(capturedUrls[1], Does.Contain("Action=GetWebIdentityToken"));
        }

        [Test]
        public void TestCreateAttestationDataWithMultiHopImpersonation()
        {
            // arrange
            var assumeRoleResponseXml1 =
                @"<AssumeRoleResponse xmlns=""https://sts.amazonaws.com/doc/2011-06-15/"">
                  <AssumeRoleResult>
                    <Credentials>
                      <AccessKeyId>intermediate-akid</AccessKeyId>
                      <SecretAccessKey>intermediate-secret</SecretAccessKey>
                      <SessionToken>intermediate-token</SessionToken>
                    </Credentials>
                  </AssumeRoleResult>
                </AssumeRoleResponse>";

            var assumeRoleResponseXml2 =
                @"<AssumeRoleResponse xmlns=""https://sts.amazonaws.com/doc/2011-06-15/"">
                  <AssumeRoleResult>
                    <Credentials>
                      <AccessKeyId>final-akid</AccessKeyId>
                      <SecretAccessKey>final-secret</SecretAccessKey>
                      <SessionToken>final-token</SessionToken>
                    </Credentials>
                  </AssumeRoleResult>
                </AssumeRoleResponse>";

            var capturedUrls = new List<string>();
            var assumeRoleCount = 0;
            var restRequester = new Mock<IRestRequester>();
            restRequester
                .Setup(r => r.Get(It.IsAny<IRestRequest>()))
                .Returns<IRestRequest>(r =>
                {
                    var url = ((RestRequestWrapper)r).ToRequestMessage(HttpMethod.Post).RequestUri.ToString();
                    capturedUrls.Add(url);
                    string body;
                    if (url.Contains("AssumeRole"))
                    {
                        assumeRoleCount++;
                        body = assumeRoleCount == 1 ? assumeRoleResponseXml1 : assumeRoleResponseXml2;
                    }
                    else
                    {
                        body = ValidGetIdentityTokenResponseXml;
                    }
                    return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) };
                });

            var awsSdkWrapper = new Mock<AwsSdkWrapper>();
            awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns("us-east-1");
            awsSdkWrapper.Setup(w => w.GetAwsCredentials()).Returns(new ImmutableCredentials("initial-akid", "initial-secret", "initial-token"));
            var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

            // act
            var attestation = retriever.CreateAttestationData(null, null,
                impersonationPath: "arn:aws:iam::111111111111:role/RoleA, arn:aws:iam::222222222222:role/RoleB");

            // assert
            Assert.AreEqual("fake.jwt.token", attestation.Credential);
            Assert.AreEqual(3, capturedUrls.Count);

            Func<string, string> expectedRoleTransform =
#if NET462 || NET471
            x => x;
            #else
            Uri.EscapeDataString;
            #endif

            Assert.That(capturedUrls[0], Does.Contain("Action=AssumeRole"));
            Assert.That(capturedUrls[0], Does.Contain(expectedRoleTransform("arn:aws:iam::111111111111:role/RoleA")));
            Assert.That(capturedUrls[1], Does.Contain("Action=AssumeRole"));
            Assert.That(capturedUrls[1], Does.Contain(expectedRoleTransform("arn:aws:iam::222222222222:role/RoleB")));
            Assert.That(capturedUrls[2], Does.Contain("Action=GetWebIdentityToken"));
        }

        [Test]
        public void TestCreateAttestationDataWithImpersonationThrowsWhenNoInitialCredentials()
        {
            // arrange
            var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
            var awsSdkWrapper = new Mock<AwsSdkWrapper>();
            awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns("us-east-1");
            awsSdkWrapper.Setup(w => w.GetAwsCredentials()).Returns((ImmutableCredentials)null);
            var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

            // act/assert
            var exception = Assert.Throws<SnowflakeDbException>(() =>
                retriever.CreateAttestationData(null, null, impersonationPath: "arn:aws:iam::123456789012:role/TestRole"));
            Assert.That(exception.Message, Does.Contain("Could not find AWS credentials"));
        }

        private static Mock<IRestRequester> CreateMockRestRequester(string responseBody)
        {
            var httpResponse = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseBody)
            };
            var restRequester = new Mock<IRestRequester>();
            restRequester
                .Setup(r => r.Get(It.IsAny<IRestRequest>()))
                .Returns(httpResponse);
            return restRequester;
        }

        private static WorkflowIdentityAwsAttestationRetriever CreateRetriever(
            IRestRequester restRequester,
            string stsHost = "https://localhost:12345",
            DateTime? utcNow = null,
            Mock<AwsSdkWrapper> awsSdkWrapper = null)
        {
            var timeProvider = new Mock<TimeProvider>();
            timeProvider.Setup(t => t.UtcNow()).Returns(utcNow ?? DateTime.UtcNow);
            var wrapper = awsSdkWrapper ?? new Mock<AwsSdkWrapper>();
            return new WorkflowIdentityAwsAttestationRetriever(timeProvider.Object,
                wrapper.Object,
                restRequester,
                stsHost);
        }
    }
}
