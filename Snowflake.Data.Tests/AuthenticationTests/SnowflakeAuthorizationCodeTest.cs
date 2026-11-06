using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests;

[Collection(nameof(AuthenticationTestsCollectionFixture))]
public class SnowflakeAuthorizationCodeTest
{
    private string _connectionString;
    private string _login;
    private string _password;

    public SnowflakeAuthorizationCodeTest(AuthenticationTestsCollectionFixture fixture)
    {
        var authTestHelper = new AuthTestHelper();
        authTestHelper.CleanBrowserProcess();
        _login = fixture.Login;
        _password = fixture.Password;

        var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeConnectionParameters();
        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
    }

    [SFFact(SkipCondition.RunOnlyOnLocal)] // "TODO SNOW-4081612"
    public void TestAuthenticateSnowflakeAuthorizationCodeSuccessful()
    {
        var authTestHelper = new AuthTestHelper();
        var connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        var provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsNotThrown();
    }

    [SFFact(SkipCondition.RunOnlyOnLocal)] // "TODO SNOW-4081612"
    public void TestAuthenticateSnowflakeAuthorizationCodeMismatchedUser()
    {
        var authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeConnectionParameters();
        parameters[SFSessionProperty.USER] = "differentUser";

        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

        var connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        var provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token.");
    }

    [SFFact(SkipCondition.RunOnlyOnLocal)] // "TODO SNOW-4081612"
    public void TestAuthenticateSnowflakeAuthorizationCodeWrongCredentials()
    {
        var authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeConnectionParameters();
        parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "15");
        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

        _login = "itsnotanaccount.com";
        _password = "fakepassword";

        var connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        var provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("fail", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 15 seconds");
    }

    [SFFact(SkipCondition.SkipOnCI)]
    public void TestAuthenticateSnowflakeAuthorizationCodeTimeout()
    {
        var authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeConnectionParameters();
        parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "1");
        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

        authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
        authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 1 seconds");
    }

    [SFFact(SkipCondition.RunOnlyOnLocal)] // "TODO SNOW-4081612"
    public void TestAuthenticateSnowflakeAuthorizationCodeWithTokenCache()
    {
        var authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeConnectionParameters();
        parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "25");
        parameters.Add(SFSessionProperty.POOLINGENABLED, "false");
        parameters[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL] = "true";
        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
        var host = parameters[SFSessionProperty.HOST];
        var port = parameters[SFSessionProperty.PORT];
        var scheme = parameters.TryGetValue(SFSessionProperty.SCHEME, out var schemeVal) ? schemeVal : "https";
        var role = parameters.TryGetValue(SFSessionProperty.ROLE, out var roleVal) ? roleVal : string.Empty;
        var tokenEndpoint = $"{scheme}://{host}:{port}/oauth/token-request";
        var connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        var provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);

        try
        {
            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsNotThrown();

            authTestHelper.CleanBrowserProcess();

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }
        finally
        {
            AuthTestHelper.RemoveTokenFromCache(tokenEndpoint, host, _login, role, TokenType.OAuthAccessToken);
            AuthTestHelper.RemoveTokenFromCache(tokenEndpoint, host, _login, role, TokenType.OAuthRefreshToken);
        }
    }

    [SFFact(SkipCondition.RunOnlyOnLocal)] // "TODO SNOW-4081612"
    public void TestAuthenticateSnowflakeAuthorizationCodeWithoutTokenCache()
    {
        var authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeConnectionParameters();
        parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "10");
        parameters.Add(SFSessionProperty.POOLINGENABLED, "false");
        parameters[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL] = "false";
        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

        var connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        var provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsNotThrown();

        authTestHelper.CleanBrowserProcess();

        authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
        authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 10 seconds");
    }
}
