using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Tests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests
{

    public class OauthConnectionTest
    {
        private string _connectionString = "";

        [SFFact(SkipCondition.SkipOnCI)]
        public void SetUp()
        {
            string token = AuthConnectionString.GetOauthToken();
            var parameters = AuthConnectionString.GetOauthConnectionString(token);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingOauthSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingOauthInvalidToken()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            string token = "invalidToken";
            var parameters = AuthConnectionString.GetOauthConnectionString(token);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Invalid OAuth access token");
        }

        [Fact(Skip = "Skipped, waits for SNOW-1893041")]
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
