using System.Threading;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests;

[Collection(nameof(AuthenticationTestsCollectionFixture))]
public class ExternalBrowserConnectionTest
{
    private string _connectionString;
    private string _login;
    private string _password;

    public ExternalBrowserConnectionTest()
    {
        var authTestHelper = new AuthTestHelper();
        _login = AuthConnectionString.SsoUser;
        _password = AuthConnectionString.SsoPassword;
        authTestHelper.CleanBrowserProcess();
        var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
    }

    [SFFact(SkipCondition.SkipOnCI)]
    public void TestAuthenticateUsingExternalBrowserSuccessful()
    {
        var authTestHelper = new AuthTestHelper();

        var connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        var provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsNotThrown();
    }

    [SFFact(SkipCondition.SkipOnCI, Skip = "SNOW-3564880")]
    public void TestAuthenticateUsingExternalBrowserMismatchedUser()
    {
        var authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
        parameters[SFSessionProperty.USER] = "differentUser";

        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

        var connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        var provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsThrown("Error: The user authenticated by the Identity Provider does not match the user specified in the Login UI. Please log out from the Identity Provider and try again.");
    }

    [SFFact(SkipCondition.SkipOnCI)]
    public void TestAuthenticateUsingExternalBrowserWrongCredentials()
    {
        var authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
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
    public void TestAuthenticateUsingExternalBrowserTimeout()
    {
        var authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
        parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "1");
        _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

        var connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        var provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("timeout", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 1 seconds");
    }
}
