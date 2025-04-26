using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests;

namespace Snowflake.Data.AuthenticationTests
{

    [NonParallelizable, IgnoreOnCI]
    public class OktaConnectionTest
    {
        private string _connectionString = "";

        [SetUp, IgnoreOnCI]
        public void SetUp()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOktaConnectionString();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
            authTestHelper.CleanBrowserProcess();

        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingOktaSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();

        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingOktaWrongUsernameParam()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOktaConnectionString();
            parameters[SFSessionProperty.USER] = "differentUser";
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("401 (Unauthorized)");
        }

        [Test, IgnoreOnCI]
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

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingOktaWrongUrl()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetOktaConnectionString();
            parameters[SFSessionProperty.AUTHENTICATOR] = "https://invalid.okta.com/";

            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("The specified authenticator is not accepted by your Snowflake account configuration");
        }


        [Test, IgnoreOnCI]
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
