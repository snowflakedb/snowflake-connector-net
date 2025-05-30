using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests;

namespace Snowflake.Data.AuthenticationTests
{

    [NonParallelizable, IgnoreOnCI]
    public class ExternalBrowserConnectionTest
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
            var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingExternalBrowserSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingExternalBrowserMismatchedUser()
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
        public void TestAuthenticateUsingExternalBrowserWrongCredentials()
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
        public void TestAuthenticateUsingExternalBrowserTimeout()
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
    }
}
