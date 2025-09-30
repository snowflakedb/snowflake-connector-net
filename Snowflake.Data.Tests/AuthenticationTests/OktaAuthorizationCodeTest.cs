using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
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

            var parameters = AuthConnectionString.GetOAuthExternalAuthorizationCodeConnectionString();
            parameters[SFSessionProperty.USER] = "differentUser";

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token");
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateOktaAuthorizationCodeWrongCredentials()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOAuthExternalAuthorizationCodeConnectionString();
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

            var parameters = AuthConnectionString.GetOAuthExternalAuthorizationCodeConnectionString();
            parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "1");
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 1 seconds");
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateOktaAuthorizationCodeWithTokenCache()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var parameters = AuthConnectionString.GetOAuthExternalAuthorizationCodeConnectionString();
            parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "10");
            parameters.Add(SFSessionProperty.POOLINGENABLED, "false");
            parameters[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL] = "true";
            var host = parameters[SFSessionProperty.HOST];

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

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
                authTestHelper.RemoveTokenFromCache(host, _login, TokenType.OAuthAccessToken);
                authTestHelper.RemoveTokenFromCache(host, _login, TokenType.OAuthRefreshToken);
            }
        }
    }
}
