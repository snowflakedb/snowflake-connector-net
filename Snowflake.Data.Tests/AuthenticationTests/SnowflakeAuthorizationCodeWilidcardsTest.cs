using System.Threading;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests
{
    [Collection(nameof(AuthenticationTestsCollectionFixture))]
    public class SnowflakeAuthorizationCodeWildcardsTest
    {
        private string _connectionString;
        private string _login;
        private string _password;

        public SnowflakeAuthorizationCodeWildcardsTest(AuthenticationTestsCollectionFixture fixture)
        {
            var authTestHelper = new AuthTestHelper();

            _login = fixture.Login;
            _password = fixture.Password;
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
            var port = parameters[SFSessionProperty.PORT];
            var scheme = parameters.TryGetValue(SFSessionProperty.SCHEME, out var schemeVal) ? schemeVal : "https";
            var role = parameters.TryGetValue(SFSessionProperty.ROLE, out var roleVal) ? roleVal : string.Empty;
            var tokenEndpoint = $"{scheme}://{host}:{port}/oauth/token-request";
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
                authTestHelper.RemoveTokenFromCache(tokenEndpoint, host, _login, role, TokenType.OAuthAccessToken);
                authTestHelper.RemoveTokenFromCache(tokenEndpoint, host, _login, role, TokenType.OAuthRefreshToken);
            }
        }
    }
}
