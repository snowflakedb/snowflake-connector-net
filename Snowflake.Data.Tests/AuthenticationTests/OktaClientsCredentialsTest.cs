using Xunit;
using Snowflake.Data.Tests;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests
{
    public class OktaClientsCredentialsTest
    {
        private string _connectionString = "";

        [Fact, IgnoreOnCI]
        public void TestAuthenticateOktaClientCredentialsSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var parameters = AuthConnectionString.GetOAuthExternalClientCredentialParameters();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }


        [Fact, IgnoreOnCI]
        public void TestAuthenticateOktaClientCredentialsMismatchedUsername()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var parameters = AuthConnectionString.GetOAuthExternalClientCredentialParameters();
            parameters[SFSessionProperty.USER] = "differentUser";
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token");
        }


        [Fact, IgnoreOnCI]
        public void TestAuthenticateOktaClientCredentialsUnauthorized()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var parameters = AuthConnectionString.GetOAuthExternalClientCredentialParameters();
            parameters[SFSessionProperty.OAUTHCLIENTID] = "invalidClientId";
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Error on getting an OAuth token from IDP:");
        }
    }
}
