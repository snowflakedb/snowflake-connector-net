using NUnit.Framework;
using Snowflake.Data.Tests;
using Snowflake.Data.Core;

namespace Snowflake.Data.AuthenticationTests
{
    [NonParallelizable, IgnoreOnCI]
    public class OktaClientsCredentialsTest
    {
        private string _connectionString = "";

        [Test, IgnoreOnCI]
        public void TestAuthenticateOktaClientCredentialsSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var parameters = AuthConnectionString.GetOAuthExternalClientCredentialParameters();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }


        [Test, IgnoreOnCI]
        public void TestAuthenticateOktaClientCredentialsMismatchedUsername()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var parameters = AuthConnectionString.GetOAuthExternalClientCredentialParameters();
            parameters[SFSessionProperty.USER] = "differentUser";
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token");
        }


        [Test, IgnoreOnCI]
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
