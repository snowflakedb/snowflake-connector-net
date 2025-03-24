using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests;

namespace Snowflake.Data.AuthenticationTests
{
    public class SnowflakeAuthorizationCodeWildcardsTest
    {
        private string _connectionString = "";
        private SFSessionProperties login_credentials;
        private string _login;
        private string _password;

        [SetUp, IgnoreOnCI]
        public void SetUp()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            _login = AuthConnectionString.SsoUser;
            _password = AuthConnectionString.SsoPassword;
            authTestHelper.CleanBrowserProcess();
            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateSnowflakeAuthorizationCodeWilidcardsSuccessful()

        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateSnowflakeAuthorizationCodeWilidcardsMismatchedUser()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            parameters[SFSessionProperty.USER] = "differentUser";

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user currently logged in at the IDP");
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateSnowflakeAuthorizationCodeWilidcardsWrongCredentials()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "15");
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            _login = "itsnotanaccount.com";
            _password = "fakepassword";

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("fail", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 15 seconds");
            }

        [Test, IgnoreOnCI]
        public void TestAuthenticateSnowflakeAuthorizationCodeWilidcardsTimeout()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "1");
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("timeout", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 1 seconds");
        }

//         // [Test, IgnoreOnCI]
//         // public void TestAuthenticateSnowflakeAuthorizationCodeTokenCache()
//         // {
//         //     AuthTestHelper authTestHelper = new AuthTestHelper();
//         //
//         //     var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
//         //     parameters.Add(SFSessionProperty.CACHEEE, "10");   //TODO: TO BE ADDED
//         //     _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
//         //
//         //     Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
//         //     Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);
//         //     authTestHelper.VerifyExceptionIsNotThrown();
//         //
//         //     parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "10");
//         //
//         //     authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
//         //
//         //     authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
//         //
//         //     authTestHelper.VerifyExceptionIsNotThrown();
//         // }
    }
}

