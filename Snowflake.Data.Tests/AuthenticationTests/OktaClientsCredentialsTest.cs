using NUnit.Framework;
using Snowflake.Data.Tests;

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

        }


        [Test, IgnoreOnCI]
        public void TestAuthenticateOktaClientCredentialsUnauthorized()

        {

        }



    }
}
