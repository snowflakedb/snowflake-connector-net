using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests;

namespace Snowflake.Data.AuthenticationTests
{

    [NonParallelizable, IgnoreOnCI]
    public class OauthConnectionTest
    {
        private string _connectionString = "";

        [SetUp, IgnoreOnCI]
        public void SetUp()
        {
            string token = AuthConnectionString.GetOauthToken();
            var parameters = AuthConnectionString.GetOauthConnectionString(token);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingOauthSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingOauthInvalidToken()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            string token = "invalidToken";
            var parameters = AuthConnectionString.GetOauthConnectionString(token);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Invalid OAuth access token");
        }

        [Test, Ignore("Skipped, waits for SNOW-1893041")]
        public void TestAuthenticateUsingOauthMismatchedUser()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            string token = AuthConnectionString.GetOauthToken();
            var parameters = AuthConnectionString.GetOauthConnectionString(token);
            parameters[SFSessionProperty.USER] = "fakeAccount";
            parameters.Add(SFSessionProperty.POOLINGENABLED, "false");
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token");
        }
    }
}
