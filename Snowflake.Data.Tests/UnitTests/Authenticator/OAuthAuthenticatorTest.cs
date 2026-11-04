using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator;

public class OAuthAuthenticatorTest
{
    [SFTheory]
    [InlineData("oauth", true)]
    [InlineData("OAUTH", true)]
    [InlineData("username_password_mfa", false)]
    public void TestRecognizeOAuthAuthenticator(string authenticator, bool expectedResult)
    {
        // act
        var result = OAuthAuthenticator.IsOAuthAuthenticator(authenticator);

        // assert
        Assert.Equal(expectedResult, result);
    }

    [SFFact]
    public void TestBuildLoginRequestDataSendsConfiguredUser()
    {
        var authenticator = CreateAuthenticator("authenticator=oauth;account=test;user=alice;token=test-token;");

        var loginRequestData = authenticator.BuildLoginRequestData();

        Assert.Equal("alice", loginRequestData.loginName);
        Assert.Equal("test-token", loginRequestData.Token);
        Assert.Equal("oauth", loginRequestData.Authenticator);
    }

    [SFFact]
    public void TestBuildLoginRequestDataAllowsOmittedUser()
    {
        var authenticator = CreateAuthenticator("authenticator=oauth;account=test;token=test-token;");

        var loginRequestData = authenticator.BuildLoginRequestData();

        Assert.Equal(string.Empty, loginRequestData.loginName);
        Assert.Equal("test-token", loginRequestData.Token);
        Assert.Equal("oauth", loginRequestData.Authenticator);
    }

    private static BaseAuthenticator CreateAuthenticator(string connectionString)
    {
        var session = new SFSession(connectionString, new SessionPropertiesContext());
        return (BaseAuthenticator)session.GetAuthenticator();
    }
}
