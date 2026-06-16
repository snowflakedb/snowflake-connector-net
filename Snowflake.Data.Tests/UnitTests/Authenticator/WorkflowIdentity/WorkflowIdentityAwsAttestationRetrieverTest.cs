using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Tests.Util;
using Xunit;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.WorkflowIdentity;

public sealed class WorkflowIdentityAwsAttestationRetrieverTest
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

    [SFTheory]
    [InlineData("us-east-1", "sts.us-east-1.amazonaws.com")]
    [InlineData("cn-northwest-1", "sts.cn-northwest-1.amazonaws.com.cn")]
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
        Assert.StartsWith($"https://{expectedHost}/?Action=GetWebIdentityToken&Version=2011-06-15", url);
        Assert.Contains("Audience.member.1=snowflakecomputing.com", url);
        Assert.Contains("SigningAlgorithm=ES384", url);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal(expectedHost, request.Headers.GetValues("Host").First());
        Assert.Equal("20250601T103000Z", request.Headers.GetValues("x-amz-date").First());
        Assert.Equal("session-token", request.Headers.GetValues("x-amz-security-token").First());
    }

    [SFFact]
    public async Task TestGetWebIdentityTokenReturnsJwt()
    {
        // arrange
        var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
        var retriever = CreateRetriever(restRequester.Object);

        // act
        var credentials = new ImmutableCredentials("akid", "secret", "session-token");
        var jwt = await retriever.GetWebIdentityTokenAsync("us-east-1", credentials, CancellationToken.None);

        // assert
        Assert.Equal("fake.jwt.token", jwt);
    }

    [SFFact]
    public async Task TestGetWebIdentityTokenThrowsWhenTokenIsEmpty()
    {
        // arrange
        var restRequester = CreateMockRestRequester(EmptyGetIdentityTokenResponseXml);
        var retriever = CreateRetriever(restRequester.Object);

        // act/assert
        var credentials = new ImmutableCredentials("akid", "secret", "session-token");
        var exception = await Assert.ThrowsAsync<SnowflakeDbException>(() => retriever.GetWebIdentityTokenAsync("us-east-1", credentials, CancellationToken.None));
        Assert.Contains("GetWebIdentityToken returned an empty token", exception.Message);
    }

    [SFFact]
    public async Task TestGetWebIdentityTokenThrowsWhenStsCallFails()
    {
        // arrange
        var restRequester = new Mock<IRestRequester>();
        restRequester
            .Setup(r => r.GetAsync(It.IsAny<IRestRequest>(), CancellationToken.None))
            .Throws(new HttpRequestException("connection refused"));
        var retriever = CreateRetriever(restRequester.Object);

        // act/assert
        var credentials = new ImmutableCredentials("akid", "secret", "session-token");
        var exception = await Assert.ThrowsAsync<SnowflakeDbException>(() => retriever.GetWebIdentityTokenAsync("us-east-1", credentials, CancellationToken.None));
        Assert.Contains("Failed to call AWS STS GetWebIdentityToken", exception.Message);
        Assert.Contains("connection refused", exception.Message);
    }

    [SFFact]
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
        Assert.Equal(AttestationProvider.AWS, attestation.Provider);
        Assert.Equal("fake.jwt.token", attestation.Credential);
        Assert.NotNull(attestation.UserIdentifierComponents);
        Assert.Equal(0, attestation.UserIdentifierComponents.Count);
    }

    [SFFact]
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
        Assert.Contains("Could not find AWS credentials", exception.Message);
    }

    [SFFact]
    public void TestCreateAttestationDataThrowsWhenNoRegion()
    {
        // arrange
        var restRequester = CreateMockRestRequester(ValidGetIdentityTokenResponseXml);
        var awsSdkWrapper = new Mock<AwsSdkWrapper>();
        awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns((string)null);
        var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

        // act/assert
        var exception = Assert.Throws<SnowflakeDbException>(() => retriever.CreateAttestationData(null, null));
        Assert.Contains("Could not find AWS region", exception.Message);
    }

    [SFFact]
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
        Assert.Equal("fake.jwt.token", attestation.Credential);
    }

    [SFFact]
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
            .Setup(r => r.GetAsync(It.IsAny<IRestRequest>(), CancellationToken.None))
            .Returns<IRestRequest, CancellationToken>((r, ct) =>
            {
                var url = ((RestRequestWrapper)r).ToRequestMessage(HttpMethod.Post).RequestUri.ToString();
                capturedUrls.Add(url);
                var body = url.Contains("AssumeRole") ? AssumeRoleResponseXml : ValidGetIdentityTokenResponseXml;
                var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) };
                return Task.FromResult(httpResponseMessage);
            });

        var awsSdkWrapper = new Mock<AwsSdkWrapper>();
        awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns("us-east-1");
        awsSdkWrapper.Setup(w => w.GetAwsCredentials()).Returns(new ImmutableCredentials("initial-akid", "initial-secret", "initial-token"));
        var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

        // act
        var attestation = retriever.CreateAttestationData(null, null, impersonationPath: "arn:aws:iam::123456789012:role/TestRole");

        // assert
        Assert.Equal("fake.jwt.token", attestation.Credential);
        Assert.Equal(2, capturedUrls.Count);
        Assert.Contains("Action=AssumeRole", capturedUrls[0]);
        Assert.Equal("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2FTestRole", capturedUrls[0]);
        Assert.Contains("Action=GetWebIdentityToken", capturedUrls[1]);
    }

    [SFFact]
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
            .Setup(r => r.GetAsync(It.IsAny<IRestRequest>(), CancellationToken.None))
            .Returns<IRestRequest, CancellationToken>((r, _) =>
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
                var result = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) };
                return Task.FromResult(result);
            });

        var awsSdkWrapper = new Mock<AwsSdkWrapper>();
        awsSdkWrapper.Setup(w => w.GetAwsRegion()).Returns("us-east-1");
        awsSdkWrapper.Setup(w => w.GetAwsCredentials()).Returns(new ImmutableCredentials("initial-akid", "initial-secret", "initial-token"));
        var retriever = CreateRetriever(restRequester.Object, awsSdkWrapper: awsSdkWrapper);

        // act
        var attestation = retriever.CreateAttestationData(null, null,
            impersonationPath: "arn:aws:iam::111111111111:role/RoleA, arn:aws:iam::222222222222:role/RoleB");

        // assert
        Assert.Equal("fake.jwt.token", attestation.Credential);
        Assert.Equal(3, capturedUrls.Count);
        Assert.Contains("Action=AssumeRole", capturedUrls[0]);
        Assert.Contains("arn%3Aaws%3Aiam%3A%3A111111111111%3Arole%2FRoleA", capturedUrls[0]);
        Assert.Contains("Action=AssumeRole", capturedUrls[1]);
        Assert.Contains("arn%3Aaws%3Aiam%3A%3A222222222222%3Arole%2FRoleB", capturedUrls[1]);
        Assert.Contains("Action=GetWebIdentityToken", capturedUrls[2]);
    }

    [SFFact]
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
        Assert.Contains("Could not find AWS credentials", exception.Message);
    }

    private static Mock<IRestRequester> CreateMockRestRequester(string responseBody)
    {
        var httpResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(responseBody),
        };
        var restRequester = new Mock<IRestRequester>();
        restRequester
            .Setup(r => r.GetAsync(It.IsAny<IRestRequest>(), CancellationToken.None))
            .Returns(Task.FromResult(httpResponse));
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
