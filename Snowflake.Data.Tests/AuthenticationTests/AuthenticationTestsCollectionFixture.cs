using Snowflake.Data.Core;
using Xunit;

namespace Snowflake.Data.AuthenticationTests;

[CollectionDefinition(nameof(AuthenticationTestsCollectionFixture),  DisableParallelization = true)]
public class AuthenticationTestsCollectionFixture : ICollectionFixture<AuthenticationTestsCollectionFixture>
{
    internal readonly string Login;
    internal readonly string Password;

    public AuthenticationTestsCollectionFixture()
    {
        var loginCredentials = AuthConnectionString.GetSnowflakeLoginCredentials();
        Login = loginCredentials[SFSessionProperty.USER];
        Password = loginCredentials[SFSessionProperty.PASSWORD];
    }
}
