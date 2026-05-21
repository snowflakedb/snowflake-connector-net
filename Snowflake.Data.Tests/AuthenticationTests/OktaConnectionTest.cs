using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Tests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests
{

    public class OktaConnectionTest
    {
        private string _connectionString = "";

        public OktaConnectionTest()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOktaConnectionString();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
            authTestHelper.CleanBrowserProcess();

        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingOktaSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();

        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingOktaWrongUsernameParam()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOktaConnectionString();
            parameters[SFSessionProperty.USER] = "differentUser";
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("401 (Unauthorized)");
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingOktaWrongCredentials()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOktaConnectionString();
            parameters[SFSessionProperty.USER] = "differentUser";
            parameters[SFSessionProperty.PASSWORD] = "fakepassword";

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("401 (Unauthorized)");
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingOktaWrongUrl()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOktaConnectionString();
            parameters[SFSessionProperty.AUTHENTICATOR] = "https://invalid.okta.com/";

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("The specified authenticator is not accepted by your Snowflake account configuration");
        }


        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingUrlWithoutOkta()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOktaConnectionString();
            parameters[SFSessionProperty.AUTHENTICATOR] = "https://invalid.abc.com/";

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Unknown authenticator");
        }
    }
}
