using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.WorkflowIdentity;

public sealed class AwsStsEndpointTest
{
    [SFTheory]
    [InlineData("us-east-1", "sts.us-east-1.amazonaws.com")]
    [InlineData("eu-west-1", "sts.eu-west-1.amazonaws.com")]
    [InlineData("cn-northwest-1", "sts.cn-northwest-1.amazonaws.com.cn")]
    public void TestRegionalEndpoint(string region, string expectedHost)
    {
        // act
        var endpoint = AwsStsEndpoint.ForRegion(region);

        // assert
        Assert.Equal(expectedHost, endpoint.Host);
        Assert.Equal($"https://{expectedHost}", endpoint.BaseUrl);
    }

    [SFTheory]
    // Bare hostname - the AWS-native form, defaulting to https
    [InlineData("sts.sc2s.sgov.gov", "sts.sc2s.sgov.gov", "https://sts.sc2s.sgov.gov")]
    [InlineData("sts.us-isob-east-1.sc2s.sgov.gov", "sts.us-isob-east-1.sc2s.sgov.gov", "https://sts.us-isob-east-1.sc2s.sgov.gov")]
    [InlineData("sts.example.com:8443", "sts.example.com:8443", "https://sts.example.com:8443")]
    // Full URL, with trailing slashes stripped
    [InlineData("https://sts.example.com", "sts.example.com", "https://sts.example.com")]
    [InlineData("https://sts.example.com/", "sts.example.com", "https://sts.example.com")]
    [InlineData("https://sts.example.com///", "sts.example.com", "https://sts.example.com")]
    // A port that is the scheme's default is dropped, so the Host header matches what is dialled
    [InlineData("https://sts.example.com:443", "sts.example.com", "https://sts.example.com")]
    // An explicit scheme is preserved so a local mock can be addressed over plain http
    [InlineData("http://localhost:12345", "localhost:12345", "http://localhost:12345")]
    // Surrounding whitespace is tolerated
    [InlineData("  sts.example.com  ", "sts.example.com", "https://sts.example.com")]
    public void TestParsesConfiguredHost(string value, string expectedHost, string expectedBaseUrl)
    {
        // act
        var parsed = AwsStsEndpoint.TryParse(value, out var endpoint, out var problem);

        // assert: no partition or suffix mapping is applied, only normalization
        Assert.True(parsed);
        Assert.Null(problem);
        Assert.Equal(expectedHost, endpoint.Host);
        Assert.Equal(expectedBaseUrl, endpoint.BaseUrl);
    }

    [SFTheory]
    [InlineData("", "is empty")]
    [InlineData("   ", "is empty")]
    [InlineData(null, "is empty")]
    // Uri rejects an absolute http(s) URI without an authority
    [InlineData("https:///path-only", "is not a valid host or URL")]
    [InlineData("://sts.example.com", "is not a valid host or URL")]
    [InlineData("ftp://sts.example.com", "must use https or http")]
    // A scheme that permits an empty authority still reports the scheme as the problem, not a missing host
    [InlineData("file:///sts", "must use https or http")]
    [InlineData("https://user:pass@sts.example.com", "must not contain user info, a query or a fragment")] // pragma: allowlist secret
    [InlineData("https://sts.example.com?Action=Foo", "must not contain user info, a query or a fragment")]
    [InlineData("https://sts.example.com#frag", "must not contain user info, a query or a fragment")]
    [InlineData("https://gateway.example.com/sts", "must not contain a path")]
    [InlineData("https://gateway.example.com/sts/", "must not contain a path")]
    // A percent-encoded trailing slash would survive trimming and become an empty path segment once the
    // query is appended, so a path is rejected outright rather than normalized
    [InlineData("https://gateway.example.com/sts%2F", "must not contain a path")]
    [InlineData("https://gateway.example.com/sts%2f", "must not contain a path")]
    public void TestRejectsValueThatCannotDescribeAnEndpoint(string value, string expectedProblem)
    {
        // act: reported, not thrown - the caller decides which error suits where the value came from
        var parsed = AwsStsEndpoint.TryParse(value, out var endpoint, out var problem);

        // assert
        Assert.False(parsed);
        Assert.Null(endpoint);
        Assert.Contains(expectedProblem, problem);
    }
}
