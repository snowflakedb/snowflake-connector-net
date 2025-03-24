using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests;

namespace Snowflake.Data.AuthenticationTests
{
    [NonParallelizable, IgnoreOnCI]
    public class OktaAuthorizationCodeTest
    {
        private string _connectionString = "";
        private string _login = AuthConnectionString.SsoUser;
        private string _password = AuthConnectionString.SsoPassword;

        [SetUp, IgnoreOnCI]
        public void SetUp()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            _login = AuthConnectionString.SsoUser;
            _password = AuthConnectionString.SsoPassword;
            authTestHelper.CleanBrowserProcess();
            var parameters = AuthConnectionString.GetOAuthExternalAuthorizationCodeConnectionString();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateOktaAuthorizationCodeSuccessful()

        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateOktaAuthorizationCodeMismatchedUser()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
            parameters[SFSessionProperty.USER] = "differentUser";

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user currently logged in at the IDP");
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateOktaAuthorizationCodeWrongCredentials()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
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
        public void TestAuthenticateOktaAuthorizationCodeTimeout()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
            parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "1");
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("timeout", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 1 seconds");
        }

        // [Test, IgnoreOnCI]
        // public void TestAuthenticateOktaAuthorizationCodeTokenCache()
        // {
        //     AuthTestHelper authTestHelper = new AuthTestHelper();
        //
        //     var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
        //     parameters.Add(SFSessionProperty.CACHEEE, "10");   //TODO: TO BE ADDED
        //     _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
        //
        //     Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        //     Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);
        //     authTestHelper.VerifyExceptionIsNotThrown();
        //
        //     parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "10");
        //
        //     authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        //
        //     authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        //
        //     authTestHelper.VerifyExceptionIsNotThrown();
        // }


    }
}
