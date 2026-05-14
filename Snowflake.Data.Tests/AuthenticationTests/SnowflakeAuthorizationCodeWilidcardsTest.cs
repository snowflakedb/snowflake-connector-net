using System.Threading;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Tests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests
{
    public class SnowflakeAuthorizationCodeWildcardsTest
    {
        private string _connectionString = "";
        private SFSessionProperties login_credentials;
        private string _login;
        private string _password;

        [SFFact(SkipCondition.SkipOnCI)]
        public void SetUp()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            login_credentials = AuthConnectionString.GetSnowflakeLoginCredentials();

            _login = login_credentials[SFSessionProperty.USER];
            _password = login_credentials[SFSessionProperty.PASSWORD];
            authTestHelper.CleanBrowserProcess();
            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateSnowflakeAuthorizationCodeWilidcardsSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateSnowflakeAuthorizationCodeWilidcardsMismatchedUser()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            parameters[SFSessionProperty.USER] = "differentUser";

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token.");
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateSnowflakeAuthorizationCodeWilidcardsWrongCredentials()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "30");
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            _login = "itsnotanaccount.com";
            _password = "fakepassword";

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("fail", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 30 seconds");
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateSnowflakeAuthorizationCodeWilidcardsTimeout()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "1");
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 1 seconds");
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateSnowflakeAuthorizationCodeWildcardsWithTokenCache()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters();
            parameters.Add(SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "10");
            parameters.Add(SFSessionProperty.POOLINGENABLED, "false");
            parameters[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL] = "true";
            var host = parameters[SFSessionProperty.HOST];
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("internalOauthSnowflakeSuccess", _login, _password);
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
