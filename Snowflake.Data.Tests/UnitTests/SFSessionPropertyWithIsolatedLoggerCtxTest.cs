using Moq;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

[CollectionDefinition(nameof(SFSessionPropertyWithIsolatedLoggerCtxTestFixture), DisableParallelization = true)]
public sealed class SFSessionPropertyWithIsolatedLoggerCtxTestFixture : ICollectionFixture<SFSessionPropertyWithIsolatedLoggerCtxTest>
{
}

[Collection(nameof(SFSessionPropertyWithIsolatedLoggerCtxTestFixture))]
public sealed class SFSessionPropertyWithIsolatedLoggerCtxTest
{
    private const string DifferentHostsWarning = "Properties OAUTHAUTHORIZATIONURL and OAUTHTOKENREQUESTURL are configured for a different host";

    [SFTheory]
    [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=http://okta.com/authorize;oauthTokenRequestUrl=https://okta.com/token-request", "Insecure OAUTHAUTHORIZATIONURL property value. It does not start with 'https://'")]
    [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=https://okta.com/authorize;oauthTokenRequestUrl=http://okta.com/token-request", "Insecure OAUTHTOKENREQUESTURL property value. It does not start with 'https://'")]
    [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;scheme=http", "Insecure SCHEME property value. Http protocol is not secure.")]
    [InlineData("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthTokenRequestUrl=https://okta.com/token-request;scheme=http", "Insecure SCHEME property value. Http protocol is not secure.")]
    [InlineData("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthTokenRequestUrl=http://okta.com/token-request;", "Insecure OAUTHTOKENREQUESTURL property value. It does not start with 'https://'")]
    public void TestWarningOnHttpCommunicationWithIdentityProviderAndSnowflakeServer(string connectionString, string expectedWarning)
    {
        // arrange
        var logger = new Mock<SFLogger>();
        var oldLogger = SFSessionProperties.ReplaceLogger(logger.Object);
        try
        {
            // act
            SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains(expectedWarning)), null), Times.Once);
        }
        finally
        {
            SFSessionProperties.ReplaceLogger(oldLogger);
        }
    }

    [SFTheory]
    [InlineData("https://okta.com/authorize", "https://other.okta.com/token-request")]
    public void TestWarningOnDifferentOAuthHosts(string authorizationUrl, string tokenUrl)
    {
        // arrange
        var logger = new Mock<SFLogger>();
        var oldLogger = SFSessionProperties.ReplaceLogger(logger.Object);
        try
        {
            var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl={authorizationUrl};oauthTokenRequestUrl={tokenUrl};";

            // act
            SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains(DifferentHostsWarning)), null), Times.Once);
        }
        finally
        {
            SFSessionProperties.ReplaceLogger(oldLogger);
        }
    }

    [SFTheory]
    [InlineData("https://okta.com/authorize", "https://okta.com/token-request")]
    public void TestNoWarningOnTheSameOAuthHosts(string authorizationUrl, string tokenUrl)
    {
        // arrange
        var logger = new Mock<SFLogger>();
        var oldLogger = SFSessionProperties.ReplaceLogger(logger.Object);
        try
        {
            var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl={authorizationUrl};oauthTokenRequestUrl={tokenUrl};";

            // act
            SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains(DifferentHostsWarning)), null), Times.Never);
        }
        finally
        {
            SFSessionProperties.ReplaceLogger(oldLogger);
        }
    }
}
